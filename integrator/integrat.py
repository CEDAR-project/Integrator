import glob
import os
import multiprocessing

# Import utilities
from util.push import Pusher
from util.sparql import SPARQLWrap

# Import modules for the pipeline
from modules.tablinker.tablinker import TabLinker
from modules.rules.rulesmaker import RuleMaker
from modules.rules.rulesinject import RulesInjector
from modules.cube.cubemaker import CubeMaker
from modules.reporting.stats import StatsGenerator

# Define the logger
import logging
log = logging.getLogger(__name__)

SHEETS_QUERY = """
PREFIX tablinker: <http://bit.ly/cedar-tablink#>
SELECT DISTINCT ?sheet FROM __RAW_DATA__ WHERE {
    ?sheet_uri a tablinker:Sheet.
    ?sheet_uri rdfs:label ?sheet.
} ORDER BY ?sheet
"""

class Integrator(object):
    def __init__(self, configuration):
        # Save the configuration
        self._conf = configuration
        
        # Create the output paths if necessary
        for path in ['raw-data', 'mappings', 'rules', 'release', 'enriched-src']:
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
        input_files = glob.glob(self._conf.get_path('source-data') + '/*.ods')
        for input_file in sorted(input_files):
            name = os.path.basename(input_file).split('.')[0]
            output_file = self._conf.get_path('raw-data') + name + '.ttl'
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
                            self._conf.get_path('raw-data'))
        
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
            task = {'dataset' : dataset,
                    'output'  : output,
                    'endpoint': self._conf.get_SPARQL(),
                    'target'  : self._conf.get_namespace('data'),
                    'raw-data': self._conf.get_graph_name('raw-data'),
                    'mappings': self._conf.get_path('mappings'),
                    'compress': self._conf.isCompress()}
            tasks.append(task)
        
        # Call rules maker in parallel, avoid hammering the store too much
        pool = multiprocessing.Pool(processes=4)
        pool.map(generate_harmonization_rules_thread, tasks)
        pool.close()
        pool.join()
        
        # Push all the data to the triple store
        self._push_to_graph(self._conf.get_graph_name('rules'),
                            self._conf.get_path('rules'))

    def generate_release(self):
        '''
        Get a list of data set to be processed and try to harmonised them into
        one big data cube
        '''
        # Prepare a task list
        tasks = []
        for sheet_name in self._get_sheets_list():
            output_file = self._conf.get_path('release') + sheet_name + '.ttl'
            task = {'sheet_name'     : sheet_name,
                    'output_file'    : output_file,
                    'endpoint'       : self._conf.get_SPARQL(),
                    'compress'       : self._conf.isCompress(),
                    'target'         : self._conf.get_namespace('data'),
                    'release_graph'  : self._conf.get_graph_name('release'),
                    'raw_data_graph' : self._conf.get_graph_name('raw-data'),
                    'rules_graph'    : self._conf.get_graph_name('rules'),
                    'measure'        : self._conf.get_measure()}
            tasks.append(task)

        # Call cube in parallel, avoid hammering the store too much
        cpu_count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=min(4, cpu_count))
        pool.map(generate_release_thread, tasks)
        pool.close()
        pool.join()
            
        # Push all the data to the triple store
        self._push_to_graph(self._conf.get_graph_name('release'),
                            self._conf.get_path('release'))
    
        # Create an instance of CubeMaker
        cubeMaker = CubeMaker(self._conf.get_SPARQL(),
                              self._conf.get_graph_name('release'),
                              self._conf.get_graph_name('raw-data'),
                              self._conf.get_graph_name('rules'))
        cubeMaker.set_target_namespace(self._conf.get_namespace('data'))
        cubeMaker.set_compress(self._conf.isCompress())
        
        # Update the DSD
        dsd_file_name = self._conf.get_path('release') + 'dsd.ttl'
        log.info("Asking CubeMaker to generate the DSD")
        cubeMaker.generate_dsd(self._conf.get_cube_title(),
                               self._conf.get_measure(),
                               self._conf.get_measureunit(),
                               self._conf.get_slices(),
                               dsd_file_name)
        
        # Load the DSD
        pusher = Pusher(self._conf.get_SPARUL(),
                        self._conf.get_user(),
                        self._conf.get_secret())
        log.info("[{}] Adding the content of the DSD".format(self._conf.get_graph_name('release')))
        if self._conf.isCompress():
            dsd_file_name = dsd_file_name + ".bz2"
        pusher.upload_file(self._conf.get_graph_name('release'), dsd_file_name)
        
    def generate_enriched_source_files(self):
        '''
        This step opens all the source files and inject the mappings rules
        back into them as annotations. This optional part of the workflow
        generates files that are useful for assessing what has been generated
        ''' 
        # Prepare a task list
        tasks = []
        input_files = glob.glob(self._conf.get_path('source-data') + '/*.ods')
        for input_file in sorted(input_files):
            base_name = os.path.basename(input_file)
            output_file = self._conf.get_path('enriched-src') + base_name
            task = {'input_file'     : input_file,
                    'output_file'    : output_file,
                    'base_name'      : base_name,
                    'endpoint'       : self._conf.get_SPARQL(),
                    'raw_data_graph' : self._conf.get_graph_name('raw-data'),
                    'rules_graph'    : self._conf.get_graph_name('rules')}
            tasks.append(task)

        # Call cube in parallel, avoid hammering the store too much
        cpu_count = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=min(4, cpu_count))
        pool.map(generate_enriched_source_files_thread, tasks)
        pool.close()
        pool.join()
    
    def generate_statistics(self):
        '''
        This pipeline step takes the data from the triple store and generate
        some statistics out of it
        '''
        statsGenerator = StatsGenerator(self._conf.get_SPARQL(),
                                self._conf.get_graph_name('raw-data'),
                                self._conf.get_graph_name('rules'),
                                self._conf.get_graph_name('release'))

        # Go !
        statsGenerator.go('stats.html')

    def _get_sheets_list(self):
        '''
        Get a list of the sheets loaded in the triple store
        '''
        sparql = SPARQLWrap(self._conf.get_SPARQL())
        params = {'__RAW_DATA__': self._conf.get_graph_name('raw-data')}
        results = sparql.run_select(SHEETS_QUERY, params)
        datasets = [sparql.format(r['sheet']) for r in results]
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
        for input_file in sorted(glob.glob(directory + '/*')):
            log.info("[{}] Loading {}".format(named_graph, input_file))
            pusher.upload_file(named_graph, input_file)
        log.info("[{}] Done loading data".format(named_graph))

def generate_raw_data_thread(parameters):
    '''
    Worker thread for generate_raw_data
    '''
    name = parameters['name']
    input_file = parameters['input_file']
    output_file = parameters['output_file']
    log.info("[{}] Calling TabLinker".format(name))
    tLinker = TabLinker(input_file, output_file, processAnnotations=True)
    tLinker.set_target_namespace(parameters['target'])
    tLinker.set_compress(parameters['compress'])
    tLinker.doLink()

def generate_harmonization_rules_thread(parameters):
    '''
    Worker thread for generate_harmonization_rules
    '''
    dataset = parameters['dataset']
    output = parameters['output']
    log.info("[{}] Calling RulesMaker".format(dataset))
    try:
        rulesMaker = RuleMaker(parameters['endpoint'], dataset, output)
        rulesMaker.set_target_namespace(parameters['target'])
        rulesMaker.set_compress(parameters['compress'])
        rulesMaker.loadMappings(parameters['mappings']) 
        rulesMaker.loadHeaders(parameters['raw-data'])
        rulesMaker.process()
    except Exception as e:
        log.error("[{}] Error in RulesMaker: {}".format(dataset.n3(), e))

def generate_release_thread(parameters):
    '''
    Worker thread for generate_release
    '''
    sheet_name = parameters['sheet_name']
    output_file = parameters['output_file']
    log.info("[{}] Calling CubeMaker".format(sheet_name))
    try:
        cubeMaker = CubeMaker(parameters['endpoint'],
                              parameters['release_graph'], 
                              parameters['raw_data_graph'], 
                              parameters['rules_graph'])
        cubeMaker.set_target_namespace(parameters['target'])
        cubeMaker.set_compress(parameters['compress'])
        cubeMaker.process(parameters['measure'], sheet_name, output_file)
    except Exception as e:
        log.error("[{}] Error in CubeMaker: {}".format(sheet_name, e))

def generate_enriched_source_files_thread(parameters):
    '''
    Worker thread for generate_enriched_source_files
    '''
    log.info("[{}] Calling RulesInjector".format(parameters['base_name']))
    try:
        rulesInjector = RulesInjector(parameters['endpoint'],
                                      parameters['rules_graph'],
                                      parameters['raw_data_graph']) 
        rulesInjector.process_workbook(parameters['input_file'], parameters['output_file'])
    except Exception as e:
        log.error("[{}] Error in RulesInjector: {}".format(parameters['base_name'], e))
    