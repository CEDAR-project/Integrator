from ConfigParser import SafeConfigParser
from rdflib.term import URIRef

import logging
log = logging.getLogger(__name__)

class Configuration(object):
    def __init__(self, configFileName):
        try :
            # Read the config file
            self.config = SafeConfigParser()
            self.config.read(configFileName)
        except :
            log.error("Could not find configuration file")
        
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
    
    def get_prefixes(self):
        '''
        prefix tablink: <http://example.org/ns/tablink#> 
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        '''
        prefixes = ""
        for (name, value) in self.namespaces.iteritems():
            prefixes = "%s prefix %s: <%s>\n" % (prefixes, name, value)
        return prefixes
        
    def curify(self, string):
        for (name, value) in self.namespaces.iteritems():
            if string.startswith(value):
                return string.replace(value, name + ':')
        return string