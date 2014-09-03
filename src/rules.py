#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, RDF, URIRef
import uuid
import bz2
import operator
import logging
import sys
import random

from codes import Codes
from common.configuration import Configuration
from common.util import clean_string
from common.sparql import SPARQLWrap
from rdflib.term import Literal

class RuleMaker(object):
    def __init__(self, configuration):
        """
        Constructor
        """
        # Keep parameters
        self.codes = Codes(configuration)
        self.conf = configuration
        self.log = logging.getLogger("RuleMaker")
    
        # Create a wrapper for SPARQL queries
        self.sparql = SPARQLWrap(self.conf)

    def _run_query(self, query, params):
        '''
        Small utility function to execute a SPARQL select
        '''
        sparql = SPARQLWrapper(self.conf.get_SPARQL())
        for (k, v) in params.iteritems():
            query = query.replace(k, v)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        sparql.setCredentials('rdfread', 'red_fred')
        results = sparql.query().convert()
        return results["results"]["bindings"]
    
    def process(self, dataset_uri, output_file):
        self.log.info("Start processing %s" % dataset_uri)
        
        # Initialise the graph
        graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)
        
        # Fix the parameters for the SPARQL queries
        query_params = {'DATASET' : dataset_uri}
        
        #####
        # Start with the column headers
        #####
        
        # Get a list of all the column headers
        # TODO fix the ugly hack with the regex
        headers = {}
        query = """
        select distinct * from <urn:graph:cedar:raw-rdf> where {
        ?header rdfs:label ?label.
        ?header a tablink:ColumnHeader.
        optional {
        ?header tablink:parentCell ?parent.
        ?parent a tablink:ColumnHeader.
        }
        filter regex(?header, "DATASET", "i")
        } """
        results = self.sparql.run_select(query, query_params)
        for result in results:
            resource = URIRef(result['header']['value'])
            headers[resource] = {}
            headers[resource]['label'] = result['label']['value']
            if 'parent' in result:
                headers[resource]['parent'] = URIRef(result['parent']['value'])
            else:
                headers[resource]['parent'] = None
        self.log.info("Process %d column headers" % len(headers))
        
        # Get a sublist of leaves
        leaves = []
        for header in headers.keys():
            ok = True
            for h in headers.keys():
                if headers[h]['parent'] == header:
                    ok = False
            if ok:
                leaves.append(header)
                
        # Process all the leaf headers, one by one
        for leaf in leaves:
            self.process_column_header(graph, dataset_uri, headers, leaf)
            
        #####
        # Move on to the row headers
        #####
        headers = {}
        query = """ 
        select distinct ?header ?label from <urn:graph:cedar:raw-rdf> where {
        ?header a tablink:RowProperty.
        ?header rdfs:label ?label.
        filter regex(?header, "DATASET", "i")
        } """
        results = self.sparql.run_select(query, query_params)
        for result in results:
            resource = URIRef(result['header']['value'])
            headers[resource] = {}
            headers[resource]['label'] = result['label']['value']
        self.log.info("Process %d row properties" % len(headers))
        
        for (header, label) in headers.iteritems():
            self.process_row_header(graph, dataset_uri, query_params, header, label)
            
        #####
        # Done
        #####
        # Write the file to disk
        if len(graph) > 0:
            self.log.info("Saving {} rules triples.".format(len(graph)))
            try :
                out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(output_file, "w")
                graph.serialize(destination=out, format='n3')
                out.close()
            except :
                self.log.error("Whoops! Something went wrong in serializing to output file")
                self.log.info(sys.exc_info())
        else:
            self.log.info("Nothing to save !")
            
    def process_row_header(self, graph, dataset_uri, query_params, header, label):
        """
        Process a row header. Get all the possible values and find a possible
        best match
        """
        labels = []
        query = """
        select distinct ?label from <GRAPH> where {
        ?obs a qb:Observation.
        ?obs <DIM> ?label.
        } order by ?label
        """.replace('DIM', header)
        results = self.sparql.run_select(query, query_params)
        for result in results:
            label = clean_string(result['label']['value'])
            if 'totaal' not in label:
                labels.append(label)
        
        # If there is no associated label forget about this header
        if len(labels) == 0:
            return
        
        # Create a small sample to detect the dimension
        size = min(20, len(labels))
        sample = [ labels[i] for i in sorted(random.sample(xrange(len(labels)), size))]
        
        # Tweak: try to see if we have a sample that correspond to years
        # see e.g. table VT_1859_02_H1
        years = True
        try:
            for entry in sample:
                y = int(entry)
                if y > 1971 or y < 1600:
                    years = False
        except ValueError:
            years = False
        # if the column contains years it should be a birth year
        if years:
            for label in labels:
                self.create_rule_set_value(graph, dataset_uri,
                                           header, Literal(label), 
                                           self.conf.getURI('cedar', 'birthYear'), Literal(label))
            return
        
        # Check if we can find the dimension associated to this header
        counts = {}
        for entry in sample:
            result = self.codes.detect_code(entry)
            if result != None:
                (dim, _) = result
                counts.setdefault(dim, 0)
                counts[dim] = counts[dim] + 1
        
        # If we can't find any possible match, skip this header        
        if len(counts) == 0:
            return
        
        # Get the dimension with the highest count
        sorted_counts = sorted(counts.iteritems(), key=operator.itemgetter(1), reverse=True)
        (dimension, _) = sorted_counts[0]
                
        # Tweak: jobPosition can not be in a hierarchical position
        # verify that the header is not the sub header of something
        # if dimension == self.conf.getURI('cedar','occupationPosition'):
        #    sparql = SPARQLWrapper(self.endpoint)
        #    query = """
        #    ask from <GRAPH> {
        #    <DIM> <http://example.org/ns#subPropertyOf> ?x.
        #    } 
        #    """.replace('GRAPH',self.namedgraph).replace('DIM', header)
        #    sparql.setQuery(query)
        #    sparql.setReturnFormat(JSON)
        #    if sparql.query().convert()['boolean']:
        #        return
        
        # Create a rule to bind the dimension to this header
        for label in labels:
            v = self.codes.get_code(dimension, label)
            if v != None:
                d = (dimension, v)
                self.create_rule_set_value(graph, dataset_uri, header, Literal(label), d)
        
    def process_column_header(self, graph, dataset_uri, headers, header):
        """
        Process a column header
        headers = set of all headers
        header = target header
        """
        # Try to find if it's a "totaal"
        header_with_total = self._contains_total(headers, header)
        if header_with_total == None:
            # The set of dimensions that will be filled in by detect_dimensions
            dimensions = set()
            
            # Try to detect dimensions in this header and those above it
            self.detect_dimensions(dimensions, headers, header)
            
            # Add all the results
            for dimension in dimensions:
                (_, dim) = dimension
                target_dim = self.conf.getURI('tablink', 'dimension')
                self.create_rule_set_value(graph, dataset_uri, target_dim, header, dim)
        else:
            # Add a rule to ignore this observation
            target_dim = self.conf.getURI('tablink', 'dimension')
            self.create_rule_ignore_observation(graph, dataset_uri, target_dim, header)
                
    
    def detect_dimensions(self, dimensions, headers, header):
        """
        Check for known labels
        """
        # Get the data
        data = headers[header]
        
        # Clean the label
        label_clean = clean_string(data['label'])
        
        # Check if we can find something that is codified
        result = self.codes.detect_code(label_clean)
        if result != None:
            dimensions.add((header, result))
        
        # Look for a birth year pattern
        # TODO: two sets of numbers with the same difference and the second
        # set higher than 1700
        
        # Recurse
        parent = data['parent']
        if parent in headers:
            # Hot fix to skip vertical merge resulting in duplicate headers
            while parent == header:
                parent = headers[parent]['parent']
            if parent in headers:
                self.detect_dimensions(dimensions, headers, parent)
    
    def _contains_total(self, headers, header):
        """
        Check if the header is about the total of something
        """
        # Get the data
        data = headers[header]
        
        # Clean the label
        label_clean = clean_string(data['label'])
        
        # Check if the label contains the string "totaal"
        if "totaal" in label_clean:
            return header
        
        # Recurse to upper level
        parent = data['parent']
        if parent in headers:
            # Hot fix to skip vertical merge resulting in duplicate headers
            while parent == header:
                parent = headers[parent]['parent']
            if parent in headers:
                return self._contains_total(headers, parent)
        
        return None
        
    def create_rule_set_value(self, graph, dataset_uri, target_dim, target_val, dimensionvalue):
        """
        Create a new harmonization rule that assign a dimension and value
        to all the observations having the targetDimension as a dimension
        """
        (dimension, value) = dimensionvalue
        resource = self.conf.getURI('cedar', 'rule-'+str(uuid.uuid1()))
        graph.add((resource,
                   RDF.type,
                   self.conf.getURI('harmonizer', 'SetValue')))
        graph.add((resource,
                   RDF.type,
                   self.conf.getURI('harmonizer', 'HarmonizationRule')))
        graph.add((resource,
                   RDF.type,
                   self.conf.getURI('prov', 'Entity')))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'targetDimension'),
                   target_dim))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'targetValue'),
                   target_val))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'targetDataset'),
                   URIRef(dataset_uri)))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'dimension'),
                   dimension))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'value'),
                   value))    
        
    def create_rule_ignore_observation(self, graph, dataset_uri, target_dim, target_val):
        """
        Create a new harmonization rule that tells to ignore observations
        associated to the target dimension
        """
        resource = self.conf.getURI('cedar', 'rule-'+str(uuid.uuid1()))
        graph.add((resource,
                   RDF.type,
                   self.conf.getURI('harmonizer', 'IgnoreObservation')))
        graph.add((resource,
                   RDF.type,
                   self.conf.getURI('harmonizer', 'HarmonizationRule')))
        graph.add((resource,
                   RDF.type,
                   self.conf.getURI('prov', 'Entity')))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'targetDimension'),
                   target_dim))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'targetValue'),
                   target_val))
        graph.add((resource,
                   self.conf.getURI('harmonizer', 'targetDataset'),
                   URIRef(dataset_uri)))
        
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    # Test
    dataset_name = "http://cedar.example.org/resource/VT_1947_A1_T_S0"
    rulesMaker = RuleMaker(config)
    rulesMaker.process(dataset_name, '/tmp/rule.ttl')
    
