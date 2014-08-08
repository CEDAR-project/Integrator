#!/usr/bin/python
from common.configuration import Configuration, RAW_XLS_PATH, MARKING_PATH,\
    RAW_RDF_PATH
import glob
import os
from tablink import TabLinker

if __name__ == '__main__':
    config = Configuration('config.ini')

    #
    # Convert the raw xls files into raw RDF data cubes
    #
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
            if not os.path.exists(dataFile):
                print name
                try:
                    tLinker = TabLinker(config, raw_xls_file, marking_file, dataFile)
                    tLinker.doLink()
                except:
                    print "Error !"
                    