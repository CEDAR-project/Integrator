#!/usr/bin/python2
from integrator.modules.tablinker.tablinker import TabLinker
from util.configuration import Configuration

import logging

if __name__ == '__main__':
    # Load the configuration file
    config = Configuration('config.ini')

    # Configure the logger
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG if config.verbose() else logging.INFO)
    logFormat = '%(asctime)s %(name)-18s %(levelname)-8s %(message)s'    
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    
    #dataset = config.getURI('cedar',"BRT_1889_02_T1-S0")
    #dataset = config.getURI('cedar',"VT_1869_01_H1-S0")
    #dataset = config.getURI('cedar','VT_1879_01_H1-S0')
    #dataset = config.getURI('cedar','VT_1859_01_H1-S6')
    filename = 'VT_1899_07_H1.ods'
    
    # Test
    tabLinker = TabLinker(config.get_path('source-data') + '/' + filename, "/tmp/data.ttl", processAnnotations=True)
    tabLinker.set_target_namespace(config.get_namespace('data'))
    tabLinker.set_compress(config.isCompress())
    tabLinker.doLink()
