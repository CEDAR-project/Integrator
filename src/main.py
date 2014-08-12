#!/usr/bin/python
from common.configuration import Configuration, RAW_XLS_PATH, MARKING_PATH, \
    RAW_RDF_PATH, RULES_PATH
import glob
import os
import bz2
from tablink import TabLinker
from push import Pusher
from rules import RuleMaker
import logging

log = logging.getLogger("Main")

def generate_raw_rdf(config):
    '''
    Convert the raw xls files into raw RDF data cubes
    '''
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
            if config.isCompress():
                dataFile = dataFile + '.bz2'
            if not os.path.exists(dataFile):
                print name
                try:
                    tLinker = TabLinker(config, raw_xls_file, marking_file, dataFile)
                    tLinker.doLink()
                except:
                    print "Error !"

def push_raw_rdf_to_virtuoso(config):
    '''
    Push the raw rdf graphs to virtuoso
    '''
    pusher = Pusher()
    raw_rdf_files = glob.glob(config.getPath(RAW_RDF_PATH) + '/*')
    for raw_rdf_file in sorted(raw_rdf_files):
        name = os.path.basename(raw_rdf_file).split('.')[0]
        named_graph = 'urn:graph:cedar:raw-rdf:' + name
        data_file = raw_rdf_file
        if data_file.endswith('.bz2'):
            uncompressedData = bz2.BZ2File(data_file).read()
            f = open('/tmp/graph.ttl', 'wb')
            f.write(uncompressedData)
            f.close()
            data_file = '/tmp/graph.ttl'
        log.info("Push " + named_graph)
        pusher.clean_graph(named_graph)
        pusher.upload_graph(named_graph, data_file)
        

def generate_harmonization_rules(config):
    '''
    Generate harmonization rules
    '''
    rulesMaker = RuleMaker(config)
    raw_rdf_files = glob.glob(config.getPath(RAW_RDF_PATH) + '/*')
    for raw_rdf_file in sorted(raw_rdf_files):
        name = os.path.basename(raw_rdf_file).split('.')[0]
        named_graph = 'urn:graph:cedar:raw-rdf:' + name
        output = config.getPath(RULES_PATH) + '/' + name + '.ttl'
        rulesMaker.process(named_graph, output)


def push_harmonization_rules_to_virtuoso(config):
    '''
    Push the rules into a named graph
    '''
    pusher = Pusher()
    named_graph = 'urn:graph:cedar:harmonization_rules'
    pusher.clean_graph(named_graph)
    
    rules_files = glob.glob(config.getPath(RULES_PATH) + '/*')
    for rules_file in sorted(rules_files):
        name = os.path.basename(rules_file).split('.')[0]
        data_file = rules_file
        if data_file.endswith('.bz2'):
            f = open('/tmp/rules.ttl', 'wb')
            f.write(bz2.BZ2File(data_file).read())
            f.close()
            data_file = '/tmp/rules.ttl'
        log.info("Add the content of " + name)
        pusher.upload_graph(named_graph, data_file)

if __name__ == '__main__':
    config = Configuration('config.ini')

    # Step 1 : combine the raw xls files and the marking information to produce raw rdf
    # generate_raw_rdf(config)
    
    # Step 2 : push all the raw rdf to the triple store
    # push_raw_rdf_to_virtuoso(config)
    
    # Step 3 : generate harmonization rules
    # generate_harmonization_rules(config)
    
    # Step 4 : push the rules to virtuoso under the named graph for the rules
    push_harmonization_rules_to_virtuoso(config)
    
