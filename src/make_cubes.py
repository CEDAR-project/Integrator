#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef
import re
import uuid
import rdflib
import bz2
import os.path

class CubeMaker(object):
    endpoint = 'http://lod.cedar-project.nl:8080/sparql/cedar'
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
    
    def __init__(self, cube_name, data):
        """
        Constructor
        """
        # Keep parameters
        self.cube_name = cube_name
        self.data = data
        self.rules = None
        
        # The graph that will be used to store the rules
        self.graph = ConjunctiveGraph()
        for namespace in self.namespaces:
            self.graph.namespace_manager.bind(namespace, self.namespaces[namespace])
        
    def go(self):
        """
        Start the job
        """
        # Iterate over all the sources
        for source in self.data['sources']:
            self.processSource(source)

    def processSource(self, source):
        """
        Process a single source of data
        """
        namedgraph = 'http://lod.cedar-project.nl/resource/v2/TABLE'.replace('TABLE', source)
        rulesFile = 'rules/' + source + '.ttl.bz2'
        
        # Load the rules file
        self.rules = rdflib.Graph()
        self.rules.load(bz2.BZ2File(rulesFile), format="turtle")
        if len(self.rules) == 0:
            return
        print "Loaded %d triple rules from %s" % (len(self.rules), rulesFile)
        
        # Load the tree of column headers
        self.dimensions = {}
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
            self.dimensions[resource] = {}
            self.dimensions[resource].setdefault('parent', []).append(parent)
        
        # TODO Associate the rules to the tree of headers under 'rules' array
        
        return     
        # Get all the observations
        sparql = SPARQLWrapper(self.endpoint)
        observations = []
        query = """
        select distinct ?obs from <GRAPH> where {
        ?obs a <http://purl.org/linked-data/cube#Observation>.
        } limit 5
        """.replace('GRAPH',namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            observations.append(URIRef(result['obs']['value']))
        print "Process %d observations" % len(observations)
        
        # Process observations
        for observation in observations:  
            self.processObservation(namedgraph, observation)
    
    def processObservation(self, namedgraph, observation):
        sparql = SPARQLWrapper(self.endpoint)
        description = {}
        query = """
        select distinct ?p ?o from <GRAPH> where {
        <OBS> ?p ?o.
        } 
        """.replace('GRAPH',namedgraph).replace('OBS',observation)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            description.setdefault(URIRef(result['p']['value']), []).append(result['o']['value'])

        self.graph.add((observation,
                        RDF.type,
                        self.namespaces['qb']['Observation']))
        if self.namespaces['tablink']['dimension'] in description:
            rule = description[self.namespaces['tablink']['dimension']]
            print rule, len(self.dimensions[rule])

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
        cubes.setdefault(cube_name, {})
        cubes[cube_name].setdefault('sources',[]).append(table)
        cubes[cube_name]['type'] = type
        cubes[cube_name]['year'] = year
        
    print "About to write {} cubes".format(len(cubes))
    
    # Go for it, one by one
    for (cube, data) in cubes.iteritems():
        if data['type'] != 'BRT' or data['year'] != '1920':
            continue
        print cube
        cube_maker = CubeMaker(cube_name, data)
        cube_maker.go()
        
    