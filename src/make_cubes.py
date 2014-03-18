#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef
import re
import uuid
import rdflib

class CubeMaker(object):
    endpoint = 'http://127.0.0.1:1234/query'
    namespaces = {
      'dcterms':Namespace('http://purl.org/dc/terms/'), 
      'skos':Namespace('http://www.w3.org/2004/02/skos/core#'), 
      'tablink':Namespace('http://example.org/ns#'), 
      'harmonizer':Namespace('http://harmonizer.example.org/ns#'),
      'rules':Namespace('http://rules.example.org/resource/'),
      'qb':Namespace('http://purl.org/linked-data/cube#'), 
      'owl':Namespace('http://www.w3.org/2002/07/owl#'),
      'sdmx-dimension':Namespace('http://purl.org/linked-data/sdmx/2009/dimension#'),
      'sdmx-code':Namespace('http://purl.org/linked-data/sdmx/2009/code#')
    }
    
    def __init__(self, type, year, cube_name, sources):
        """
        Constructor
        """
        # Keep parameters
        self.type = type
        self.year = year
        self.cube_name = cube_name
        self.sources = sources
        
        # The graph that will be used to store the rules
        self.graph = ConjunctiveGraph()
        for namespace in self.namespaces:
            self.graph.namespace_manager.bind(namespace, self.namespaces[namespace])
        
    def go(self):
        """
        Start the job
        """
        # Iterate over all the sources
        for source in self.sources:
            self.processSource(source)

    def processSource(self, source):
        """
        Process a single source of data
        """
        namedgraph = 'http://localhost:8080/data/TABLE.rdf'.replace('TABLE', table)
        
        # Load the tree of column headers
        headers = {}
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?header ?parent from <GRAPH> where {
        ?header a <http://example.org/ns#ColumnHeader>.
        ?header <http://example.org/ns#subColHeaderOf> ?parent.
        }
        """.replace('GRAPH',namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            resource = result['header']['value']
            parent = result['parent']['value']
            headers[parent] = {}
            headers[parent].setdefault('child', []).append(resource)
        
        # Load the rules associated to each header
        g = rdflib.Graph()
        g.load(bz2.BZ2File('rules/' + table + '.ttl.bz2'), format="turtle")
        print "Loaded %d triples" % len(g)
            
        # Get all the observations
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?obs ?p ?o from <GRAPH> where {
        ?obs a <http://purl.org/linked-data/cube#Observation>.
        ?obs ?p ?o.
        }
        """.replace('GRAPH',namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            self.processObservation(None, None)
    
    def processObservation(self, observation, description):
        pass
    
if __name__ == '__main__':
    # table = 'BRT_1899_10_T_marked'
    # Load the list of tables
    tables = [table.strip() for table in open('tables.txt')]
    
    # We are going to make one cube per type/year combination, map the sources
    cubes = {}
    for table in tables:
        t = table.split('_')
        type = t[0]
        year = t[1]
        cube_name = "{}-{}".format(type, year)
        cubes.setdefault(cube_name, []).append(table)
    print "About to write {} cubes".format(len(cubes))
    
    
    # Go for it, one by one
    for (cube, sources) in cubes.iteritems():
        cube_maker = CubeMaker(type, year, cube_name, sources)
        cube_maker.go()
        
    