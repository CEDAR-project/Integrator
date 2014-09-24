#!/usr/bin/python2
import uuid
import bz2
import operator
import logging
import sys
import csv
import datetime
import os

from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, RDF, URIRef
from common.configuration import Configuration
from common.util import clean_string
from common.sparql import SPARQLWrap
from rdflib.term import Literal
from rdflib.namespace import RDFS

class Codes(object):
    
    def __init__(self, configuration):
        self.conf = configuration
        self.log = configuration.getLogger("RuleMaker")
        
        # Declare the mappings
        self.mappings = {}
        self.mappings['sex'] = {
            'predicate' : self.conf.getURI('sdmx-dimension', 'sex'),
            'mappings' : 'data/input/mapping/sex.csv'
        }
        self.mappings['maritalstatus'] = {
            'predicate' : self.conf.getURI('maritalstatus', 'maritalStatus'),
            'mappings' : 'data/input/mapping/marital_status.csv'
        }
        self.mappings['occupationPosition'] = {
            'predicate' : self.conf.getURI('cedar', 'occupationPosition'),
            'mappings' : 'data/input/mapping/occupation_position.csv'
        }
        self.mappings['occupation'] = {
            'predicate' : self.conf.getURI('cedar', 'occupation'),
            'mappings' : 'data/input/mapping/occupation.csv'
        }
        self.mappings['belief'] = {
            'predicate' : self.conf.getURI('cedar', 'belief'),
            'mappings' : 'data/input/mapping/belief.csv'
        }
        self.mappings['city'] = {
            'predicate' : self.conf.getURI('sdmx-dimension', 'refArea'),
            'mappings' : 'data/input/mapping/city.csv'
        }
        self.mappings['province'] = {
            'predicate' : self.conf.getURI('sdmx-dimension', 'refArea'),
            'mappings' : 'data/input/mapping/province.csv'
        }
        
        # Load the content of the files
        for mapping in self.mappings.values():
            self.log.debug("Loading %s ..." % mapping['mappings']) 
            mapping['map'] = dict()   
            f = open(mapping['mappings'], "rb")
            reader = csv.reader(f)
            header_row = True
            for row in reader:
                # Skip the header
                if header_row:
                    header_row = False
                    continue
                # Skip empty lines
                if len(row) != 2:
                    continue
                if row[1].startswith("http"):
                    mapping['map'][row[0]] = URIRef(row[1])
                else:
                    mapping['map'][row[0]] = Literal(row[1])
            f.close()
    
    def get_mapping_types(self):
        """
        Return a list of all the mappings loaded
        """
        return self.mappings.keys()
    
    def get_mapping_src_URI(self, mapping_type):
        """
        Return a URI for the file source
        """
        if mapping_type not in self.mappings:
            return None
        
        fileName = self.mappings[mapping_type]['mappings']
        root = URIRef("https://github.com/CEDAR-project/Integrator/raw/master/")
        mappingsDumpURI = root + os.path.relpath(fileName)
        return URIRef(mappingsDumpURI)
    
    def get_code_for(self, mapping_type, literal):
        """
        Return the code associated to a literal or None
        """
        if mapping_type not in self.mappings:
            return None
        
        mappings = self.mappings[mapping_type]['map']        
        if literal not in mappings:
            return None
        return (self.mappings[mapping_type]['predicate'], mappings[literal])

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
    
    def process(self, sheet_uri, output_file):
        '''
        Function used to process one of the sheets in the data sets
        '''
        self.log.info("Start processing sheet %s" % sheet_uri)
        
        # Initialise the graph
        graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)
        
        # Fix the parameters for the SPARQL queries
        query_params = {'SHEET' : sheet_uri,
                        'GRAPH' : self.conf.get_graph_name('raw-data')}

        # We look for two type of headers
        search_keys = [
            {'target':'tablink:ColumnHeader', 'parent': 'tablink:ColumnHeader'},
            {'target':'tablink:RowHeader', 'parent': 'tablink:RowProperty'}
        ]
        
        # Start describing the activity
        activity_URI = URIRef(sheet_uri + '-mapping-activity')
        
        # Keep the start time
        startTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            datatype=self.conf.getURI('xsd', 'dateTime'))
        
        for key in search_keys:
            # Precise query parameters
            query_params['TARGET'] = key['target']
            query_params['PARENT'] = key['parent']
            
            # Get a list of all the column headers
            headers = {}
            query = """
            select distinct * from <GRAPH> where {
                ?header a TARGET.
                ?header tablink:value ?label.
                ?header tablink:sheet <SHEET>.
            optional {
                ?header tablink:parentCell ?parent.
                ?parent a PARENT.
                }
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
                    
            self.log.info("Process %d %s" % (len(headers), key['target']))
            used_mappings = self.process_headers(graph, activity_URI, headers)
            
        # Keep the end time
        endTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            datatype=self.conf.getURI('xsd', 'dateTime'))
        
        # Finish describing the activity
        graph.add((activity_URI,
                   RDF.type,
                   self.conf.getURI('prov', 'Activity')))
        graph.add((activity_URI,
                   self.conf.getURI('prov', 'startedAtTime'),
                   startTime))
        graph.add((activity_URI,
                   self.conf.getURI('prov', 'endedAtTime'),
                   endTime))
        graph.add((activity_URI,
                   self.conf.getURI('prov', 'wasAssociatedWith'),
                   URIRef("https://github.com/CEDAR-project/Integrator")))
        for dim_type in used_mappings:
            target = self.codes.get_mapping_src_URI(dim_type)
            if target != None:
                graph.add((activity_URI,
                           self.conf.getURI('prov', 'used'),
                           target))
        
        # Write the file to disk
        if len(graph) > 0:
            self.log.info("Saving {} rules triples.".format(len(graph)))
            try :
                out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(output_file, "w")
                graph.serialize(destination=out, format='n3')
                out.close()
            except :
                self.log.error("Whoops! Something went wrong in serialising to output file")
                self.log.info(sys.exc_info())
        else:
            self.log.info("Nothing to save !")

    def _map_to_date(self, value):
        # Tweak: try to see if we have a sample that correspond to years
        # see e.g. table VT_1859_02_H1
        try:
            y = int(value)
            if y > 1971 or y < 1600:
                return (self.conf.getURI('cedar', 'birthYear'), Literal(y))
        except ValueError:
            return None
    
    def _is_ignored(self, value):
        if 'totaal' in value:
            return True
        return False
    
    def process_headers(self, graph, activity_URI, headers):
        used_mappings = set()
        
        # Bag the headers according to their parents
        header_bags = {}
        for (header, data) in headers.iteritems():
            parent = 'root' if data['parent'] == None else data['parent']
            header_bags.setdefault(parent, {'content':[]})
            header_bags[parent]['content'].append(header)
            
        # Build a map to assign a possible value to every header
        header_mapping = {}
        for (header, data) in headers.iteritems():
            header_mapping[header] = {}
            label = clean_string(data['label'])
            
            # See if we should maybe ignore this point, set a flag for it
            data['ignore'] = self._is_ignored(label)
            
            if not data['ignore']:
                # Try to map the value to a birth date
                header_mapping[header]['birthdate'] = self._map_to_date(label)
                
                # Try to map the value to any other standard code
                for map_type in self.codes.get_mapping_types():
                    header_mapping[header][map_type] = self.codes.get_code_for(map_type, label)
                
        # Iterate over the bags and look for the most popular binding
        for header_bag in header_bags.values():
            total = {}
            for header in header_bag['content']:
                if not headers[header]['ignore']: 
                    for (dimension_type, binding) in header_mapping[header].iteritems():
                        total.setdefault(dimension_type, 0)
                        if binding != None:
                            total[dimension_type] = total[dimension_type] + 1
            sorted_total = sorted(total.iteritems(), key=operator.itemgetter(1), reverse=True)
            header_bag['best_match'] = None if len(sorted_total) == 0 else sorted_total[0]

        # Annotate all the headers
        for header_bag in header_bags.values():
            for header in header_bag['content']:
                if headers[header]['ignore']:
                    # If the header is ignored annotate it with a specific dimension
                    binding = (self.conf.getURI('cedar', 'ignore'), Literal("1"))
                    self._annotate_header(graph, activity_URI, header, binding)
                else:
                    if header_bag['best_match'] != None:
                        (dim_type, total) = header_bag['best_match']
                        # See if we have enough confidence
                        if total >= len(header_bag['content']) * 0.25:
                            # See if we have a mapping for this header
                            binding = header_mapping[header][dim_type]
                            if binding != None:
                                used_mappings.add(dim_type)
                                self._annotate_header(graph, activity_URI, header, binding)
                            
        # Return the list of mappings used
        return used_mappings

    def _annotate_header(self, graph, activity_URI, header, binding):
        resource = header + '-mapping'
        body = resource + '-body'
        (p, o) = binding
        graph.add((resource,
                   RDF.type,
                   self.conf.getURI('oa', 'Annotation')))
        graph.add((resource,
                   self.conf.getURI('oa', 'hasTarget'),
                   header))
        graph.add((resource,
                   self.conf.getURI('prov', 'wasGeneratedBy'),
                   activity_URI))
        graph.add((resource,
                   self.conf.getURI('oa', 'serializedBy'),
                   URIRef("https://github.com/CEDAR-project/Integrator")))
        graph.add((resource,
                   self.conf.getURI('oa', 'serializedAt'),
                   Literal(datetime.datetime.now().strftime("%Y-%m-%d"), datatype=self.conf.getURI('xsd', 'date'))))
        graph.add((resource,
                   self.conf.getURI('oa', 'hasBody'),
                   body))
        graph.add((body,
                   RDF.type,
                   RDFS.Resource))
        graph.add((body,
                   p,
                   o))
        
    def create_rule_set_value(self, graph, dataset_uri, target_dim, target_val, dimensionvalue):
        """
        Create a new harmonization rule that assign a dimension and value
        to all the observations having the targetDimension as a dimension
        """
        (dimension, value) = dimensionvalue
        resource = self.conf.getURI('cedar', 'rule-' + str(uuid.uuid1()))
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
        
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    # Test
    # dataset_name = "http://cedar.example.org/resource/VT_1947_A1_T_S0"
    sheet_name = "http://lod.cedar-project.nl:8888/cedar/resource/BRT_1930_07_S3_S0"
    sheet_name = "http://lod.cedar-project.nl:8888/cedar/resource/VT_1840_00_S6_S2"
    rulesMaker = RuleMaker(config)
    rulesMaker.process(sheet_name, '/tmp/rule.ttl')
    
