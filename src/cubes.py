#!/usr/bin/python2
from rdflib import ConjunctiveGraph, Literal, RDF
from codes import Codes
from common.configuration import Configuration
from common.sparql import SPARQLWrap
from rdflib.namespace import XSD
import bz2
import sys
import logging

RULES_GRAPH = 'urn:graph:cedar:harmonization_rules'
    
class CubeMaker(object):
    def __init__(self, configuration):
        """
        Constructor
        """
        self.log = logging.getLogger("CubeMaker")
        
        # Keep parameters
        self.conf = configuration
        
        # Load the codes
        self.codes = Codes(configuration)

        # Create a wrapper for SPARQL queries
        self.sparql = SPARQLWrap(self.conf)
    
    def save_data(self):
        '''
        Save all additional files into ttl files. Contains data that span
        over all the processed raw cubes
        '''
        pass
    
    def process(self, dataset_uri, output_file):        
        """
        Process all the observations in the dataset and look for rules to
        harmonize them, save the output into outputfile_name
        """
        # The graph that will be used to store the cube
        graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)

        # Get a list of observations for this data set
        observations = {}
        query = """
        select distinct ?obs ?p ?o from <urn:graph:cedar:raw-rdf> where {
        ?obs a qb:Observation.
        ?obs qb:dataSet <DATASET>.
        ?obs ?p ?o.
        } order by ?obs
        """
        results = self.sparql.run_select(query, {'DATASET' : dataset_uri})
        for result in results:
            observation = self.sparql.format(result['obs'])
            p = self.sparql.format(result['p'])
            o = self.sparql.format(result['o'])
            if p != RDF.type:
                observations.setdefault(observation, [])
                observations[observation].append((p,o))
        
        self.log.info("Process %d observations" % len(observations))
        
        # Get the associated rules
        rules = {}
        query = """
        select distinct * from <urn:graph:cedar:harmonization_rules> where {
        ?rule a ?type .
        ?rule harmonizer:targetDimension ?targetdim .
        ?rule harmonizer:targetValue ?targetval .
        ?rule harmonizer:targetDataset <DATASET> .
        optional {
            ?rule harmonizer:dimension ?dim .
            ?rule harmonizer:value ?val .
        }}
        """
        results = self.sparql.run_select(query, {'DATASET' : dataset_uri})
        for result in results:
            rule = {
                    'type': self.sparql.format(result['type']),
                    'target_dim' : self.sparql.format(result['targetdim']),
                    'target_value' : self.sparql.format(result['targetval'])
                    }
            if 'dim' in result and 'val' in result:
                d = self.sparql.format(result['dim'])
                v = self.sparql.format(result['val'])
                rule['dv'] = (d,v)
            key = rule['target_dim'].n3() + "_" + rule['target_value'].n3()
            rules.setdefault(key, [])
            rules[key].append(rule)
        
        self.log.info("With %d rules" % len(rules))
        
        # Process observations
        for (observation, description) in observations.iteritems():
            # Get an harmonised observation
            harmonized_po = self._process_observation(rules, description)
            
            # If obs is different from None we have a new data point
            if harmonized_po != None:
                # Add the new observation to the graph
                resource = observation + '-h'
                graph.add((resource,
                           RDF.type,
                           self.conf.getURI('qb','Observation')))
                graph.add((resource,
                           self.conf.getURI('prov','wasDerivedFrom'),
                           observation))
                for (p, o) in harmonized_po:
                    graph.add((resource, p, o))

        # Write the file to disk
        if len(graph) > 0:
            self.log.info("Saving {} data triples.".format(len(graph)))
            try :
                out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(output_file, "w")
                graph.serialize(destination=out, format='n3')
                out.close()
            except :
                self.log.error("Whoops! Something went wrong in serializing to output file")
                self.log.info(sys.exc_info())
        else:
            self.log.info("Nothing to save !")
            
    def _process_observation(self, rules, description):
        '''
        Process a specific source observation and add to the graph the
        harmonized version of it
        '''
        # The new observation will be a list of tuples (p,o)
        observation = []
        
        # Check that the observation is an int, skip it otherwise (a bit hacky now)
        # http://example.org/resource/BRT_1920_01_S1_marked/populationSize
        pop_size = self.conf.getURI('tablink', 'value')
        pop_size_val = None
        for (p, o) in description:
            if p == pop_size:
                pop_size_val = o
        if pop_size_val == None:
            return None
        #try:
        #    pop_size_val = int(pop_size_val.toPython())
        #except ValueError:
        #    return None
        # print type(pop_size_val)
        
        observation.append((self.conf.getURI('cedar', 'population'), Literal(pop_size_val, datatype=XSD.decimal)))
        
        for (dim,value) in description:
            # Get the set of rules matching this observation
            key = dim.n3() + "_" + value.n3()
            if key in rules:
                for rule in rules[key]:
                    if rule['type'] == self.conf.getURI('harmonizer', 'IgnoreObservation'):
                        return None
                    elif rule['type'] == self.conf.getURI('harmonizer', 'SetValue'):
                        observation.append(rule['dv'])
        return observation
    
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    # Get the name of a data set to test
    # BRT_1889_05_T4_S0 -> 43 obs
    # BRT_1889_05_T5_S0 -> 221 obs
    graph = "urn:graph:cedar:raw-rdf:VT_1947_A1_T"
    sparql = SPARQLWrap(config)
    query = """
    select distinct ?ds from <GRAPH> where {
    ?ds a qb:DataSet.
    } order by ?ds limit 1
    """
    result = sparql.run_select(query, {'GRAPH' : graph})[0]
    dataset_uri = sparql.format(result['ds'])

    # Test
    cube = CubeMaker(config)
    cube.process(dataset_uri, "/tmp/data.ttl")
    
