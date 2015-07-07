from ConfigParser import SafeConfigParser
from rdflib.term import URIRef

import logging
import glob
import os
log = logging.getLogger(__name__)

class Configuration(object):
    def __init__(self, configFileName):
        try :
            # Read the config file
            self.config = SafeConfigParser()
            self.config.read(configFileName)
        except Exception as e:
            log.error("Error loading the configuration file : " + e)
        
    def isCompress(self):
        return self.config.get('debug', 'compress') == '1';
    
    def verbose(self):
        return self.config.get('debug', 'verbose') == '1';
    
    def get_graph_name(self, name):
        return URIRef(self.config.get('graphs', name)).n3()

    def get_path(self, path):
        return self.config.get('paths', path)
        
    def get_SPARQL(self):
        return self.config.get('general', 'sparql_endpoint')
    
    def get_SPARUL(self):
        return self.config.get('general', 'sparul_endpoint')
    
    def get_user(self):
        return self.config.get('general', 'sparul_user')
    
    def get_secret(self):
        return self.config.get('general', 'sparul_secret')
    
    def get_namespace(self, name):
        return self.config.get('namespaces', name)
    
    def get_slices(self):
        res = []
        slices = [s for s in self.config.sections() if s.startswith('slice-')]
        for s in slices:
            slice_def = {}
            slice_def['title']    = self.config.get(s, 'title')
            sources = glob.glob(self.get_path('source-data') + '/' + 
                                    self.config.get(s, 'sources'))
            files = [os.path.basename(src) for src in sources]
            slice_def['sources']  = files
            slice_def['property'] = self.config.get(s, 'property')
            slice_def['value']    = self.config.get(s, 'value')
            res.append(slice_def)
        return res
    
    def get_measure(self):
        return self.config.get('cube', 'measure')

    def get_measureunit(self):
        return self.config.get('cube', 'measureunit')

    def get_cube_title(self):
        return self.config.get('cube', 'title')
    