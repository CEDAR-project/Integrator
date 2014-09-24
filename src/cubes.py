#!/usr/bin/python2
from rdflib import ConjunctiveGraph, Literal, RDF
from common.configuration import Configuration
from common.sparql import SPARQLWrap
from rdflib.namespace import XSD, RDFS
import bz2
import sys
import logging
from rdflib.term import BNode, URIRef

RULES_GRAPH = 'urn:graph:cedar:harmonization_rules'

# TODO: If the value is not an int mark the point as being ignored
# TODO: When getting the RDF model from the construct, look for dimensions used

class CubeMaker(object):
    def __init__(self, configuration):
        """
        Constructor
        """
        self.log = logging.getLogger("CubeMaker")
        
        # Keep parameters
        self.conf = configuration
        
        # Create a wrapper for SPARQL queries
        self.sparql = SPARQLWrap(self.conf)
        
        # Keep a set of all the dimensions encountered
        self._dimensions = set()
        
        # The URI of the harmonized data set
        self._harmonized_uri = configuration.getURI('cedar','harmonized-data') 
        
        # Keep a list of slices
        self._slices = set()
        
    def save_data(self, output_file):
        '''
        Save all additional files into ttl files. Contains data that span
        over all the processed raw cubes
        '''
        # The graph that will be used to store the cube
        graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)
        
        # Create the data set
        graph.add((self._harmonized_uri,
                   RDF.type,
                   self.conf.getURI('qb','DataSet')))
        graph.add((self._harmonized_uri,
                   RDF.type,
                   self.conf.getURI('prov','Entity')))
        graph.add((self._harmonized_uri,
                   self.conf.getURI('dcterms','title'),
                   Literal("Harmonized census data 1795-1971")))
        graph.add((self._harmonized_uri,
                   self.conf.getURI('rdfs','label'),
                   Literal("Harmonized census data 1795-1971")))
        for s in self._slices:
            graph.add((self._harmonized_uri,
                       self.conf.getURI('qb','slice'),
                       s))
            
        # Finish describing the slices
        slicestruct_uri = self._harmonized_uri + '-sliced-by-type-and-year'
        graph.add((slicestruct_uri,
                   RDF.type,
                   self.conf.getURI('qb','SliceKey')))
        graph.add((slicestruct_uri,
                   RDFS.label,
                   Literal("Slice by census type and census year")))
        graph.add((slicestruct_uri,
                   self.conf.getURI('qb','componentProperty'),
                   self.conf.getURI('cedar','censusType')))
        graph.add((slicestruct_uri,
                   self.conf.getURI('qb','componentProperty'),
                   self.conf.getURI('sdmx-dimension','refPeriod')))
        for s in self._slices:
            (census_type,census_year) = s.split('-')[-1].split('_')
            graph.add((s,
                       RDF.type,
                       self.conf.getURI('qb','Slice')))
            graph.add((s,
                       self.conf.getURI('sdmx-dimension','refPeriod'),
                       Literal(int(census_year))))
            graph.add((s,
                       self.conf.getURI('cedar','censusType'),
                       Literal(census_type)))
            graph.add((s,
                       self.conf.getURI('qb','sliceStructure'),
                       slicestruct_uri))

        # Create a DSD
        dsd = self._harmonized_uri + '-dsd'
        graph.add((self._harmonized_uri,
                   self.conf.getURI('qb','structure'),
                   dsd))
        graph.add((dsd,
                   RDF.type,
                   self.conf.getURI('qb','DataStructureDefinition')))
        graph.add((dsd,
                   self.conf.getURI('sdmx-attribute','unitMeasure'),
                   URIRef('http://dbpedia.org/resource/Natural_number')))
        ## dimensions
        ### all the encountered dimensions
        order = 1
        for dim in self._dimensions:
            dim_uri = BNode()
            graph.add((dsd,
                       self.conf.getURI('qb','component'),
                       dim_uri))
            graph.add((dim_uri,
                       self.conf.getURI('qb','dimension'),
                       dim))
            graph.add((dim_uri,
                       self.conf.getURI('qb','order'),
                       Literal(order)))
            order = order + 1
        ### the ref period used in the slices
        dim_uri = BNode()
        graph.add((dsd,
                   self.conf.getURI('qb','component'),
                   dim_uri))
        graph.add((dim_uri,
                   self.conf.getURI('qb','dimension'),
                   self.conf.getURI('sdmx-dimension','refPeriod')))
        graph.add((dim_uri,
                   self.conf.getURI('qb','order'),
                   Literal(order)))
        graph.add((dim_uri,
                   self.conf.getURI('qb','componentAttachment'),
                   self.conf.getURI('qb','Slice')))
        order = order + 1
        ### the census type used in the slices
        dim_uri = BNode()
        graph.add((dsd,
                   self.conf.getURI('qb','component'),
                   dim_uri))
        graph.add((dim_uri,
                   self.conf.getURI('qb','dimension'),
                   self.conf.getURI('cedar','censusType')))
        graph.add((dim_uri,
                   self.conf.getURI('qb','order'),
                   Literal(order)))
        graph.add((dim_uri,
                   self.conf.getURI('qb','componentAttachment'),
                   self.conf.getURI('qb','Slice')))
        order = order + 1
        ## measure
        measure_uri = BNode()
        graph.add((dsd,
                   self.conf.getURI('qb','component'),
                   measure_uri))
        graph.add((measure_uri,
                   self.conf.getURI('qb','measure'),
                   self.conf.getURI('cedar', 'population')))
        ## attributes
        attr_uri = BNode()
        graph.add((dsd,
                   self.conf.getURI('qb','component'),
                   attr_uri))
        graph.add((attr_uri,
                   self.conf.getURI('qb','attribute'),
                   self.conf.getURI('sdmx-attribute','unitMeasure')))
        graph.add((attr_uri,
                   self.conf.getURI('qb','componentRequired'),
                   Literal("true", datatype=XSD.boolean)))
        graph.add((attr_uri,
                   self.conf.getURI('qb','componentAttachment'),
        ## slice key
                   self.conf.getURI('qb','DataSet')))
        graph.add((dsd,
                   self.conf.getURI('qb','sliceKey'),
                   slicestruct_uri))
        
        self.log.info("Saving {} extra data triples.".format(len(graph)))
        try :
            out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(output_file, "w")
            graph.serialize(destination=out, format='n3')
            out.close()
        except :
            self.log.error("Whoops! Something went wrong in serializing to output file")
            self.log.info(sys.exc_info())
    
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
                    'id': self.sparql.format(result['rule']),
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
                resource = observation + '-harmonized'
                graph.add((resource,
                           RDF.type,
                           self.conf.getURI('qb','Observation')))
                graph.add((resource,
                           RDF.type,
                           self.conf.getURI('prov','Entity')))
                graph.add((resource,
                           self.conf.getURI('prov','wasDerivedFrom'),
                           observation))
                graph.add((resource,
                           self.conf.getURI('qb','dataSet'),
                           self._harmonized_uri))
                for (p, o) in harmonized_po:
                    self._dimensions.add(p)
                    graph.add((resource, p, o))
                    

                # Add it to the relevant slice
                key = '_'.join(dataset_uri.split('/')[-1].split('_')[:2]) 
                slice_uri = self._harmonized_uri + '-slice-' + key
                self._slices.add(slice_uri)
                graph.add((slice_uri,
                           self.conf.getURI('qb','observation'),
                           resource))
                
                # Describe a related activity
                activity = observation + '-activity'
                graph.add((resource,
                           self.conf.getURI('prov','wasGeneratedBy'),
                           activity))
                graph.add((activity,
                           RDF.type,
                           self.conf.getURI('prov','Activity')))
                for values in rules.itervalues():
                    for v in values:
                        graph.add((activity,
                                   self.conf.getURI('prov','used'),
                                   v['id']))
                

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
    
    # Set the name of a data set to test
    # BRT_1889_05_T4_S0 -> 43 obs
    # BRT_1889_05_T5_S0 -> 221 obs
    # VT_1947_A1_T
    dataset_uri = config.getURI('cedar', 'BRT_1889_05_T4_S0')

    # Test
    cube = CubeMaker(config)
    cube.process(dataset_uri, "/tmp/data.ttl")
    cube.save_data("/tmp/extra.ttl")
    
