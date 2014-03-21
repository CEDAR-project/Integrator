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
        self.rules = {}
        
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
            # See if we have harmonization rules for this file
            rulesFile = 'rules/' + table + '.ttl.bz2'
            if not os.path.isfile(rulesFile):
                print 'No harmonization rules for {0} !'.format(source)
                continue
            self.processSource(source, rulesFile)

    def processSource(self, source, rulesFile):
        """
        Process a single source of data
        """
        namedgraph = 'http://lod.cedar-project.nl/resource/v2/TABLE'.replace('TABLE', source)
        
        # Load the rules file
        g = rdflib.Graph()
        g.load(bz2.BZ2File(rulesFile), format="turtle")
        if len(g) == 0:
            return
        print "Loaded %d triple rules from %s" % (len(self.rules), rulesFile)
        
        # Parse the RDF to associate the rules to the dimensions
        #qres = g.query(
        
        return
    
        # Get all the observations in several pages
        observations = []
        offset = 0
        page_size = 100
        has_next_page = True
        while has_next_page:
            sparql = SPARQLWrapper(self.endpoint)
            query = """
            select distinct ?obs from <GRAPH> where {
            ?obs a <http://purl.org/linked-data/cube#Observation>.
            } 
            """.replace('GRAPH',namedgraph)
            #query = query + " limit " + page_size + " offset " + offset
            query = query + " limit 5"
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            nb_results = 0
            for result in results["results"]["bindings"]:
                nb_results = nb_results + 1
                observations.append(URIRef(result['obs']['value']))
            offset = offset + page_size
            has_next_page = (nb_results == page_size)
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
        
    