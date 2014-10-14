#!/usr/bin/python2
import glob
import os
import multiprocessing

from common.configuration import Configuration
from common.push import Pusher
from common.sparql import SPARQLWrap
from tablink import TabLink
from rules import RuleMaker
from cubes import CubeMaker

# All the paths
RAW_XLS_FILES = "data/input/raw-xls/*.xls"  # Raw XLS files fetched from EASY
MARKING_FILES = "data/input/marking/*.txt"  # Annotations for the files
MAPPING_FILES = "data/input/codes/*.csv"  # All the mapping
RAW_RDF_PATH = "data/output/raw-rdf/"  # The raw RDF for the marked XLS
H_RULES_PATH = "data/output/rules/"  # The harmonisation rules
RELEASE_PATH = "data/output/release/"  # The released data set

config = Configuration('config.ini')
log = config.getLogger("Main")


def get_datasets_list():
    global config
    global log
    
    datasets = []
    sparql = SPARQLWrap()
    query = """
    select distinct ?ds from <%s> where {
    ?ds a tablink:Sheet.
    } order by ?ds
    """ % config.get_graph_name('raw-data')
    results = sparql.run_select(query, None)
    for result in results:
        datasets.append(sparql.format(result['ds']))
    return datasets

def generate_raw_rdf():
    '''
    Convert the raw xls files into raw RDF data cubes
    '''
    global config
    global log
    
    # Prepare a task list
    tasks = []
    
    # Go check all the files one by one, push a task if needed
    marking_index = {}
    for marking_file in glob.glob(MARKING_FILES):
        name = os.path.basename(marking_file).split('.')[0]
        marking_index[name] = marking_file        
    for raw_xls_file in sorted(glob.glob(RAW_XLS_FILES)):
        name = os.path.basename(raw_xls_file).split('.')[0]
        if name in marking_index:
            marking_file = marking_index[name]
            dataFile = RAW_RDF_PATH + name + '.ttl'
            dataFileCheck = dataFile
            if config.isCompress():
                dataFileCheck = dataFileCheck + '.bz2'
            if (not os.path.exists(dataFileCheck)) or config.isOverwrite():
                task = {'name':name,
                        'raw_xls_file':raw_xls_file,
                        'marking_file':marking_file,
                        'dataFile':dataFile}
                tasks.append(task)
    
    # Call tablinker in parallel
    pool_size = multiprocessing.cpu_count() * 3
    pool = multiprocessing.Pool(processes=pool_size)
    pool.map(generate_raw_rdf_thread, tasks)
    pool.close()
    pool.join()
    
def generate_raw_rdf_thread(parameters):
    global config
    global log
    
    name = parameters['name']
    raw_xls_file = parameters['raw_xls_file']
    marking_file = parameters['marking_file']
    dataFile = parameters['dataFile']
    try:
        log.info("Calling tablinker for %s" % name)
        tLinker = TabLink(config, raw_xls_file, marking_file, dataFile)
        tLinker.doLink()
    except:
        log.error("Can not process %s" % name)
    
def generate_harmonization_rules():
    '''
    Generate harmonization rules
    '''
    global config
    global log
    
    # Prepare a task list
    tasks = []
    
    # Prepare to process each data set
    for dataset in get_datasets_list():
        name = dataset.split('/')[-1]
        output = H_RULES_PATH + '/' + name + '.ttl'
        task = {'dataset' : dataset, 'output' : output}
        tasks.append(task)
    
    # Call rules maker in parallel, avoid hammering the store too much
    pool = multiprocessing.Pool(processes=4)
    pool.map(generate_harmonization_rules_thread, tasks)
    pool.close()
    pool.join()
    
def generate_harmonization_rules_thread(parameters):
    global config
    global log
    
    dataset = parameters['dataset']
    output = parameters['output']
    log.info("Process " + dataset.n3())
    rulesMaker = RuleMaker(config)
    rulesMaker.process(dataset, output)
        
def create_harmonized_dataset():
    '''
    Get a list of data set to be processed and try to harmonised them into
    one big cube
    '''
    global config
    global log
    
    # Prepare a task list
    tasks = []
    
    cube = CubeMaker(config)
    for dataset in get_datasets_list(config):
        name = dataset.split('/')[-1]
        data_file = RELEASE_PATH + name + '.ttl'
        data_file_check = data_file
        if config.isCompress():
            data_file_check = data_file_check + '.bz2'
        if (not os.path.exists(data_file_check)) or config.isOverwrite():
            task = {'dataset' : dataset,
                    'data_file' : data_file,
                    'cube' : cube}
            tasks.append(task)

    # Call cube in parallel, avoid hammering the store too much
    pool = multiprocessing.Pool(processes=4)
    pool.map(create_harmonized_dataset_thread, tasks)
    pool.close()
    pool.join()
    
    log.info("Save additional data")
    cube.save_data(RELEASE_PATH + 'extra.ttl')
    
def create_harmonized_dataset_thread(parameters):
    global config
    global log
    
    cube = parameters['cube']
    dataset = parameters['dataset']
    data_file = parameters['data_file']
    try:
        log.info("Process " + dataset.n3())
        cube.process(dataset, data_file)
    except:
        log.error("Can not process %s" % dataset.n3())
    
def push_to_virtuoso(named_graph, directory):
    '''
    Push data to virtuoso
    '''
    global config
    global log
    
    pusher = Pusher(config.get_SPARQL())
    log.info("Clean " + named_graph)
    pusher.clean_graph(named_graph)
    log.info("Push the content of " + directory)
    pusher.upload_directory(named_graph, directory)

if __name__ == '__main__':
    # Create the output paths if necessary
    if not os.path.exists(RAW_RDF_PATH):
        os.makedirs(RAW_RDF_PATH)
    if not os.path.exists(H_RULES_PATH):
        os.makedirs(H_RULES_PATH)
    if not os.path.exists(RELEASE_PATH):
        os.makedirs(RELEASE_PATH)
        
    # Step 1 : combine the raw xls files and the marking information to produce raw rdf
    #generate_raw_rdf()
    
    # Step 2 : push all the raw rdf to the triple store
    #push_to_virtuoso(config.get_graph_name('raw-data'), RAW_RDF_PATH + '/*')
    
    # Step 3 : generate harmonisation rules
    generate_harmonization_rules()
    
    # Step 4 : push the rules to virtuoso under the named graph for the rules
    push_to_virtuoso(config.get_graph_name('rules'), H_RULES_PATH + '/*')
    
    # Step 5 : get the observations from all the cube and try to harmonize them
    create_harmonized_dataset()
    
    # Step 6 : push the harmonized data and all additional files to the release
    push_to_virtuoso(config.get_graph_name('release'), RELEASE_PATH + '/*')
    
