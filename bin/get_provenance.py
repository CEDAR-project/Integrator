#!/usr/bin/python2
import argparse
import sys
import os.path
import rdflib
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                '../integrator')))
from util.configuration import Configuration
from rdflib.graph import ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper
from modules.tablinker.namespace import PROV

DESCRIBE_QUERY = """
    CONSTRUCT {__RESOURCE__ ?p ?o.}
    FROM __RELEASE__
    FROM __RULES__
    FROM __RAW_DATA__
    WHERE {__RESOURCE__ ?p ?o.}
"""

import logging
logger = logging.getLogger(__name__)

class ProvenanceTracker(object):
    def __init__(self, configuration):
        self.conf = configuration
        
    def track(self, resource):
        graph = ConjunctiveGraph()
        sparql = SPARQLWrapper(self.conf.get_SPARQL())
        
        queue = [resource]
        while len(queue) != 0:
            target = queue.pop()   
            query = DESCRIBE_QUERY.replace('__RESOURCE__', target.n3())
            query = query.replace('__RELEASE__', self.conf.get_graph_name('release'))
            query = query.replace('__RULES__', self.conf.get_graph_name('rules'))
            query = query.replace('__RAW_DATA__', self.conf.get_graph_name('raw-data'))
            sparql.setQuery(query)
            results = sparql.query().convert()
            for statement in results:
                # Add the statement to the graph
                graph.add(statement)
                
                # If the relate to another resource we describe, queue it
                (_,p,o) = statement
                if p.startswith(PROV):
                    if o.startswith(self.conf.get_namespace('data')):
                        queue.append(o)
                    
        print graph.serialize(format='turtle')
        
        
# Example of usage :
# /home/cgueret/Code/CEDAR/DataDump-mini-vt/config.ini VT_1879_01_H1-S0-S1019-h

if __name__ == '__main__':
    # Configure a logger
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.INFO)
    logFormat = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'    
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(logFormat))
    root_logger.addHandler(ch)
    
    # Parse the command line
    parser = argparse.ArgumentParser(description='Extract the provenance of an harmonized observation')
    parser.add_argument('configuration', metavar='configuration', type=str,
                        help='The configuration file used for Integrator')
    parser.add_argument('resource', metavar='resource', type=str,
                        help='The resource to track the provenance of')
    args = parser.parse_args()
    
    # Get the provenance of the resource
    configuration = Configuration(args.configuration)
    data_ns = rdflib.namespace.Namespace(configuration.get_namespace('data'))
    provTracker = ProvenanceTracker(configuration)
    provTracker.track(data_ns[args.resource])

    