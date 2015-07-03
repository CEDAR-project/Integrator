#!/usr/bin/python2
from modules.rules.rulesmaker import RuleMaker
from util.configuration import Configuration

import logging

if __name__ == '__main__':
    # Load the configuration file
    config = Configuration('config-cedar.ini')

    # Configure the logger
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG if config.verbose() else logging.INFO)
    logFormat = '%(asctime)s %(name)-18s %(levelname)-8s %(message)s'    
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    fh = logging.FileHandler('rules.log')
    fh.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(fh)
    
    #dataset = config.getURI('cedar',"BRT_1889_02_T1-S0")
    #dataset = config.getURI('cedar',"VT_1869_01_H1-S0")
    #dataset = config.getURI('cedar','VT_1879_01_H1-S0')
    #dataset = config.getURI('cedar','VT_1859_01_H1-S6')
    dataset = 'VT_1899_07_H1-S0'
    
    # Test
    rulesMaker = RuleMaker(config.get_SPARQL(), dataset, "/tmp/test.ttl")
    rulesMaker.loadMappings(config.get_path('mappings')) #, ['Sex','MaritalStatus']
    rulesMaker.loadHeaders(config.get_graph_name('raw-data'))
    rulesMaker.process() # ['Sex','MaritalStatus']
