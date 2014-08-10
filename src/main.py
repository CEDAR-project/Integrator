#!/usr/bin/python
from common.configuration import Configuration, RAW_XLS_PATH, MARKING_PATH,\
    RAW_RDF_PATH
import glob
import os
import bz2
from tablink import TabLinker
from push import Pusher

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
        print named_graph
        pusher = Pusher(named_graph, data_file)
        pusher.upload_graph()
        
if __name__ == '__main__':
    config = Configuration('config.ini')

    #generate_raw_rdf(config)
    #push_raw_rdf_to_virtuoso(config)
    