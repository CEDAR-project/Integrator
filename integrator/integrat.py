import glob
import os
import multiprocessing

# Import utilities
from util.push import Pusher
from util.sparql import SPARQLWrap

# Import modules for the pipeline
from modules.tablinker.tablinker import TabLinker
from modules.rules.rulesmaker import RuleMaker
from modules.cubemaker.cubes import CubeMaker

# Define the logger
import logging
log = logging.getLogger(__name__)

SHEETS_QUERY = """
PREFIX tablinker: <http://bit.ly/cedar-tablink#>
SELECT DISTINCT ?name FROM __RAW_DATA__ WHERE {
    ?sheet a tablinker:Sheet.
    ?sheet rdfs:label ?name.
} ORDER BY ?name
"""

class Integrator(object):
    def __init__(self, configuration):
        # Save the configuration
        self._conf = configuration
        
        # Create the output paths if necessary
        for path in ['raw-data', 'mappings', 'rules', 'release']:
            if not os.path.exists(self._conf.get_path(path)):
                os.makedirs(self._conf.get_path(path))
            
    def generate_raw_data(self):
        '''
        Convert the input annotated spreadsheet files into raw RDF tabular data.
        This function uses multi-processing to process several file in parallel.
        At the end all the data is pushed to the triple store
        '''
        # Prepare a task list
        tasks = []
        
        # Go check all the files one by one, push a task if needed
        input_files = glob.glob(self._conf.get_path('source-data') + '/*.ods')
        for input_file in sorted(input_files):
            name = os.path.basename(input_file).split('.')[0]
            output_file = self._conf.get_path('raw-data') + name + '.ttl'
            out_check = output_file + '.bz2' if self._conf.isCompress() else output_file
            if (not os.path.exists(out_check)) or self._conf.isOverwrite():
                task = {'name'       :name,
                        'input_file' :input_file,
                        'output_file':output_file,
                        'target'     :self._conf.get_namespace('data'),
                        'compress'   :self._conf.isCompress()}
                tasks.append(task)
        
    
        # Call tablinker in parallel
        pool_size = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=pool_size)
        pool.map(generate_raw_data_thread, tasks)
        pool.close()
        pool.join()
        
        # Push everything to the triple store
        self._push_to_graph(self._conf.get_graph_name('raw-data'),
                            self._conf.get_path('raw-data') + '/*')
        
    def generate_harmonization_rules(self):
        '''
        Generate harmonisation rules based on the data sets found in the raw-rdf
        data and the mapping rules in the mappings directory
        '''
        # Prepare a task list
        tasks = []
        
        # Prepare to process each data set
        for dataset in self._get_sheets_list():
            name = dataset.split('/')[-1]
            output = self._conf.get_path('rules') + '/' + name + '.ttl'
            task = {'dataset' : dataset, 'output' : output}
            tasks.append(task)
        
        def generate_harmonization_rules_thread(parameters):
            dataset = parameters['dataset']
            output = parameters['output']
            log.info("[{}] Calling RulesMaker".format(dataset.n3()))
            try:
                rulesMaker = RuleMaker(dataset, output)
                rulesMaker.loadMappings(self._conf.get_path('mappings')) 
                rulesMaker.loadHeaders(True)
                rulesMaker.set_compress(self._conf.isCompress())
                rulesMaker.process()
            except:
                log.error("[{}] Error in RulesMaker".format(dataset.n3()))

        # Call rules maker in parallel, avoid hammering the store too much
        pool = multiprocessing.Pool(processes=4)
        pool.map(generate_harmonization_rules_thread, tasks)
        pool.close()
        pool.join()
        
        # Push all the data to the triple store
        self._push_to_graph(self._conf.get_graph_name('rules'),
                            self._conf.get_path('rules') + '/*')

    def generate_release(self):
        '''
        Get a list of data set to be processed and try to harmonised them into
        one big data cube
        '''
        # Erase previous DSD
        dsd_file = self._conf.get_path('release') + '/dsd.ttl'
        if self._conf.isCompress():
            dsd_file = dsd_file + '.bz2'
        if os.path.exists(dsd_file):
            os.remove(dsd_file)
            
        # Prepare a task list
        tasks = []
        for dataset in self._get_sheets_list():
            name = dataset.split('/')[-1]
            data_file = self._conf.get_path('release') + name + '.ttl'
            data_file_check = data_file
            if self._conf.isCompress():
                data_file_check = data_file_check + '.bz2'
            if (not os.path.exists(data_file_check)) or self._conf.isOverwrite():
                task = {'dataset' : dataset,
                        'data_file' : data_file}
                tasks.append(task)
    
        def generate_release_thread(parameters):
            dataset = parameters['dataset']
            data_file = parameters['data_file']
            log.info("[{}] Calling CubeMaker".format(dataset.n3()))
            try:
                cubeMaker = CubeMaker()
                cubeMaker.process(dataset, data_file)
            except:
                log.error("[{}] Error in CubeMaker".format(dataset.n3()))

        # Call cube in parallel, avoid hammering the store too much
        cpu_count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=min(4, cpu_count))
        pool.map(generate_release_thread, tasks)
        pool.close()
        pool.join()
            
        # Push all the data to the triple store
        self._push_to_graph(self._conf.get_graph_name('release'),
                            self._conf.get_path('release') + '/*')
    
        # Update the DSD
        dsd_file_name = self._conf.get_path('release') + '/dsd.ttl'
        log.info("Asking CubeMaker to generate the DSD")
        cubeMaker = CubeMaker()
        cubeMaker.generate_dsd(dsd_file_name)
        
        # Load the DSD
        pusher = Pusher(self._conf.get_SPARUL())
        log.info("Adding the DSD to the triple store")
        pusher.upload_file(self._conf.get_graph_name('release'), dsd_file_name)
        self._add_to_graph()
    
    def generate_enhanced_source_files(self):
        pass
    
    def _get_sheets_list(self):
        '''
        Get a list of the sheets loaded in the triple store
        '''
        sparql = SPARQLWrap(self._conf.get_SPARQL())
        params = {'__RAW_DATA__': self._conf.get_graph_name('raw-data')}
        results = sparql.run_select(SHEETS_QUERY, params)
        datasets = [sparql.format(r['name']) for r in results]
        return datasets
    
    def _push_to_graph(self, named_graph, directory):
        '''
        Push data to to the triple store
        '''
        pusher = Pusher(self._conf.get_SPARUL(),
                        self._conf.get_user(),
                        self._conf.get_secret())
        log.info("[{}] Cleaning the content of the graph ".format(named_graph))
        pusher.clean_graph(named_graph)
        log.info("[{}] Loading files in {}".format(named_graph, directory))
        for input_file in sorted(glob.glob(directory)):
            pusher.upload_file(named_graph, input_file)

def generate_raw_data_thread(parameters):
    '''
    Worker thread for generate raw_data
    '''
    name = parameters['name']
    input_file = parameters['input_file']
    output_file = parameters['output_file']
    target = parameters['target']
    compress = parameters['compress']
    log.info("[{}] Calling TabLinker".format(name))
    tLinker = TabLinker(input_file, output_file, processAnnotations=True)
    tLinker.set_target_namespace(target)
    tLinker.set_compress(compress)
    tLinker.doLink()
