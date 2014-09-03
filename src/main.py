#!/usr/bin/python2
from common.configuration import Configuration, RAW_XLS_PATH, MARKING_PATH, \
    RAW_RDF_PATH, RULES_PATH
import glob
import os
import logging
from tablink import TabLinker
from common.push import Pusher
from rules import RuleMaker
from common.sparql import SPARQLWrap
from cubes import CubeMaker
import multiprocessing

log = logging.getLogger("Main")

def get_datasets_list(config):
    datasets = []
    sparql = SPARQLWrap(config)
    query = """
    select distinct ?ds from <urn:graph:cedar:raw-rdf> where {
    ?ds a qb:DataSet.
    ?ds tablink:sheetName ?name .
    ?ds qb:structure ?s .
    } order by ?ds
    """
    results = sparql.run_select(query, None)
    for result in results:
        datasets.append(sparql.format(result['ds']))
    return datasets

def generate_raw_rdf(config):
    '''
    Convert the raw xls files into raw RDF data cubes
    '''
    # Prepare a task list
    tasks = []
    
    # Go check all the files one by one, push a task if needed
    raw_xls_files = glob.glob(config.getPath(RAW_XLS_PATH))
    marking_files = glob.glob(config.getPath(MARKING_PATH))
    marking_index = {}
    for marking_file in marking_files:
        name = os.path.basename(marking_file).split('.')[0]
        marking_index[name] = marking_file        
    for raw_xls_file in sorted(raw_xls_files):
        name = os.path.basename(raw_xls_file).split('.')[0]
        if name in marking_index:
            marking_file = marking_index[name]
            dataFile = config.getPath(RAW_RDF_PATH) + name + '.ttl'
            dataFileCheck = dataFile
            if config.isCompress():
                dataFileCheck = dataFileCheck + '.bz2'
            if not os.path.exists(dataFileCheck):
                task = {'name':name,
                        'config':config,
                        'raw_xls_file':raw_xls_file,
                        'marking_file':marking_file,
                        'dataFile':dataFile}
                tasks.append(task)
    
    # Call tablinker in parallel
    pool_size = multiprocessing.cpu_count() * 2
    pool = multiprocessing.Pool(processes=pool_size)
    pool.map(generate_raw_rdf_thread, tasks)
    pool.close()
    pool.join()
    
def generate_raw_rdf_thread(parameters):
    name = parameters['name']
    raw_xls_file = parameters['raw_xls_file']
    marking_file = parameters['marking_file']
    config = parameters['config']
    dataFile = parameters['dataFile']
    try:
        log.info("Calling tablinker for %s" % name)
        tLinker = TabLinker(config, raw_xls_file, marking_file, dataFile)
        tLinker.doLink()
    except:
        log.error("Can not process %s" % name)
    
def generate_harmonization_rules(config):
    '''
    Generate harmonization rules
    '''
    # Prepare a task list
    tasks = []
    
    # Prepare to process each data set
    for dataset in get_datasets_list(config):
        name = dataset.split('/')[-1]
        output = config.getPath(RULES_PATH) + '/' + name + '.ttl'
        task = {'dataset' : dataset, 'output' : output}
        tasks.append(task)
    
    # Call rules maker in parallel, avoid hammering the store too much
    pool = multiprocessing.Pool(processes=4)
    pool.map(generate_harmonization_rules_thread, tasks)
    pool.close()
    pool.join()
    
def generate_harmonization_rules_thread(parameters):
    dataset = parameters['dataset']
    output = parameters['output']
    log.info("Process " + dataset.n3())
    rulesMaker = RuleMaker(config)
    rulesMaker.process(dataset, output)
        
def create_harmonized_dataset(config):
    '''
    Get a list of data set to be processed and try to harmonised them into
    one big cube
    '''
    # !!!! Not thread safe yet
    cube = CubeMaker(config)
    for dataset in get_datasets_list(config):
        name = dataset.split('/')[-1]
        data_file = 'data/output/release/' + name + '.ttl'
        data_file_check = data_file
        if config.isCompress():
            data_file_check = data_file_check + '.bz2'
        if not os.path.exists(data_file_check):
            try:
                log.info("Process " + dataset.n3())
                cube.process(dataset, data_file)
            except:
                log.error("Can not process %s" % name)
    log.info("Save additional data")
    cube.save_data('data/output/release/extra.ttl')
    
def push_to_virtuoso(config, named_graph, directory):
    '''
    Push data to virtuoso
    '''
    pusher = Pusher()
    log.info("Clean " + named_graph)
    pusher.clean_graph(named_graph)

    data_files = glob.glob(directory)
    for data_file in sorted(data_files):
        log.info("Add the content of " + data_file)
        pusher.upload_graph(named_graph, data_file)


if __name__ == '__main__':
    config = Configuration('config.ini')

    # Step 1 : combine the raw xls files and the marking information to produce raw rdf
    # generate_raw_rdf(config)
    
    # Step 2 : push all the raw rdf to the triple store
    # push_to_virtuoso(config, 'urn:graph:cedar:raw-rdf', config.getPath(RAW_RDF_PATH) + '/*')
    
    # Step 3 : generate harmonisation rules
    # generate_harmonization_rules(config)
    
    # Step 4 : push the rules to virtuoso under the named graph for the rules
    # push_to_virtuoso(config, 'urn:graph:cedar:harmonization_rules', config.getPath(RULES_PATH) + '/*')
    
    # Step 5 : get the observations from all the cube and try to harmonize them
    # create_harmonized_dataset(config)
    
    # Step 6 : push the harmonized data and all additional files to the release
    # push_to_virtuoso(config, 'urn:graph:cedar:harmonised_data', 'data/output/release/*')
    
