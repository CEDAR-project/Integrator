#!/usr/bin/python2
import bz2
import datetime
import os
import pprint
import csv

from rdflib import ConjunctiveGraph, RDF, URIRef
from common.configuration import Configuration
from common.sparql import SPARQLWrap
from rdflib.term import Literal
from rdflib.namespace import RDFS
from ConfigParser import SafeConfigParser
from xlrd import open_workbook
from xlutils.margins import number_of_good_cols, number_of_good_rows
from common import util

import sys
reload(sys)
sys.setdefaultencoding("utf8")  # @UndefinedVariable

pp = pprint.PrettyPrinter(indent=2)

HEADERS_QUERY = 'Queries/get-headers.rq'
HEADERS_CACHE_DIR = '_cache/'
INTEGRATOR_URI = URIRef("https://github.com/CEDAR-project/Integrator")

class MappingsList(object):
    
    def __init__(self, data):
        self._mappings = {}
        
        self.excelFileName = data['file']
        predicate = URIRef(data['predicate'])
        mapping_type = data['mapping_type']
        
        # Load the mappings
        wb = open_workbook(data['path'] + "/" + self.excelFileName, formatting_info=False, on_demand=True)
        sheet = wb.sheet_by_index(0)
        colns = number_of_good_cols(sheet)
        rowns = number_of_good_rows(sheet)
        for i in range(1, rowns):
            # Do we have a specific target ?
            target = sheet.cell(i, 0).value
            
            # Get the string (force reading the cell as a string)
            literal = sheet.cell(i, 1).value
            if type(literal) == type(1.0):
                literal = str(int(literal))
            literal = util.clean_string(literal)
            
            # Get the values
            values = []
            for j in range(2, colns):
                value = sheet.cell(i, j).value
                if value != '':
                    # Codes using numbers need to be seen as string
                    if type(value) == type(1.0):
                        value = str(int(value))
                        
                    # Encode the value
                    encoded_value = None
                    if mapping_type == 'uri':
                        prefix = data['prefix']
                        encoded_value = URIRef(prefix + value)
                    elif mapping_type == 'boolean':
                        isTrue = (value == '1' or value == 'true')
                        encoded_value = Literal(isTrue)
                    else:
                        encoded_value = Literal(value)
                        
                    # Prefix the code and pair with predicate
                    pair = (predicate, encoded_value)
                    values.append(pair)
                    
            # Save the mapping
            if len(values) > 0:
                self._mappings.setdefault(literal + target, {})
                self._mappings[literal + target] = values
        
    def get_src_URI(self):
        """
        Return a URI for the Excel file
        """
        root = URIRef("https://raw.githubusercontent.com/CEDAR-project/DataDump/master/mapping/")
        mappingsDumpURI = root + os.path.relpath(self.excelFileName)
        return URIRef(mappingsDumpURI)
    
    def get_file_name(self):
        return self.excelFileName
    
    def get_mappings_for(self, literal, target):
        '''
        Returns a set of pairs for a given string
        '''
        # If there is a specific mapping for this target return that one
        if literal + target in self._mappings:
            return self._mappings[literal + target]
        
        # If we have a mapping that works for all datasets return it
        if literal in self._mappings:
            return self._mappings[literal]
         
        # We have nothing
        return None
    
class RuleMaker(object):
    def __init__(self, configuration, dataset, output_file_name):
        """
        Constructor
        """
        self.conf = configuration
        self.log = self.conf.getLogger("RuleMaker")
        self.dataset = dataset
        self.output_file_name = output_file_name
        self.mappings = {}
        self.headers = []
        
        # Create a wrapper for SPARQL queries
        self.sparql = SPARQLWrap(self.conf)

    def process(self, dims=None):
        '''
        Function used to process one of the sheets in the data sets
        '''
        if dims == None:
            dims = sorted(self.mappings.keys())
            
        # The graph for the results
        graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)
        
        # Mint a URI for the activity
        activity_URI = URIRef(self.dataset + '-mapping-activity')

        # Keep the start time
        startTime = self._now()
            
        self.log.info("Start processing %s" % dims)
        for dim in dims:
            self.log.debug("=> %s" % dim)
            count = self._process_mapping(graph, activity_URI, dim)
            
            if count != 0:
                # Describe the file used
                mappingFileDSURI = activity_URI + '-' + dim 
                mappingFileDistURI = mappingFileDSURI + '-dist'
                graph.add((activity_URI, self.conf.getURI('prov', 'used'), mappingFileDSURI))
                graph.add((mappingFileDSURI, RDF.type, self.conf.getURI('dcat', 'Dataset')))
                graph.add((mappingFileDSURI, RDFS.label, Literal(dim)))
                graph.add((mappingFileDSURI, self.conf.getURI('dcat', 'distribution'), mappingFileDistURI))
                graph.add((mappingFileDistURI, RDF.type, self.conf.getURI('dcat', 'Distribution')))
                graph.add((mappingFileDistURI, RDFS.label, Literal(self.mappings[dim].get_file_name())))
                graph.add((mappingFileDistURI, self.conf.getURI('dcterms', 'accessURL'), self.mappings[dim].get_src_URI()))
            
        # Keep the end time
        endTime = self._now()
        
        # Finish describing the activity
        graph.add((activity_URI, RDF.type, self.conf.getURI('prov', 'Activity')))
        graph.add((activity_URI, RDFS.label, Literal("Annotate")))
        graph.add((activity_URI, self.conf.getURI('prov', 'startedAtTime'), startTime))
        graph.add((activity_URI, self.conf.getURI('prov', 'endedAtTime'), endTime))
        graph.add((activity_URI, self.conf.getURI('prov', 'wasAssociatedWith'), INTEGRATOR_URI))

        # Save the graph
        self.log.info("Saving {} data triples.".format(len(graph)))
        try :
            out = bz2.BZ2File(self.output_file_name + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(self.output_file_name, "w")
            graph.serialize(destination=out, format='n3')
            out.close()
        except :
            self.log.error(self.basename + "Whoops! Something went wrong in serialising to output file")
            
    def _process_mapping(self, graph, activity_URI, dimension_name):
        count = 0
        
        # Process all the headers one by one
        mappings_list = self.mappings[dimension_name]
        for header in self.headers:
            [cell_name, literal, _, dataset_name] = header
            pairs = mappings_list.get_mappings_for(literal, dataset_name)
            if pairs != None:
                count = count + 1
                # Mint URIs
                annotation_URI = URIRef(cell_name + "-mapping")
                annotation_body_URI = annotation_URI + "-body"
                # Add the triples
                graph.add((annotation_URI, RDF.type, self.conf.getURI('oa', 'Annotation')))
                graph.add((annotation_URI, RDFS.label, Literal('Mapping')))
                graph.add((annotation_URI, self.conf.getURI('oa', 'hasBody'), annotation_body_URI))
                graph.add((annotation_URI, self.conf.getURI('oa', 'hasTarget'), URIRef(cell_name)))
                graph.add((annotation_URI, self.conf.getURI('oa', 'serializedAt'), self._now()))
                graph.add((annotation_URI, self.conf.getURI('oa', 'serializedBy'), INTEGRATOR_URI))
                graph.add((annotation_URI, self.conf.getURI('prov', 'wasGeneratedBy'), activity_URI))
                graph.add((annotation_body_URI, RDF.type, RDFS.Resource))
                graph.add((annotation_body_URI, RDFS.label, Literal('Mapping body')))
                for pair in pairs:
                    (p,o) = pair
                    graph.add((annotation_body_URI, p, o))
                     
        return count
               
    def _now(self):
        '''
        Return the current time formated as a typed Literal
        '''
        return Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), 
                       datatype=self.conf.getURI('xsd', 'dateTime'))
        
    def loadHeaders(self, refresh=False):
        '''
        This method fetches all the header used in the raw data and
        saves them as a cache in a CSV file
        @input refresh is used to force the refresh of the cache
        '''
        self.log.info("[%s] Loading headers" % self.dataset)
        
        # Compose the name of the cache file
        cache_file_name = HEADERS_CACHE_DIR + self.dataset.split('/')[-1] + '-headers.csv'
        
        # See if we have a cached copy of the headers 
        # or if we must reload them anyway
        if not os.path.exists(cache_file_name) or refresh:
            self.log.debug("[%s] => Refreshing the cache" % self.dataset)
            
            # Create the cache directory if necessary
            if not os.path.exists(os.path.dirname(cache_file_name)):
                os.makedirs(os.path.dirname(cache_file_name))
            
            # Load and execute the SPARQL query, save to the cache too
            sparql_query = open(HEADERS_QUERY, 'r').read()
            sparql_params = {'__DATA_SET__' : self.dataset.n3() }
            cache_file = open(cache_file_name, 'wb')
            csv_writer = csv.writer(cache_file, delimiter=';', quotechar='"')
            results = self.sparql.run_select(sparql_query, sparql_params)
            for result in results:
                # Parse the result
                cell_name = result['cell']['value']
                header_type = result['header_type']['value']
                dataset_name = result['dataset_name']['value']
                # TODO remove the clean string once done up-front by tablink
                literal = util.clean_string(result['literal']['value'])
                row = [cell_name, literal, header_type, dataset_name]
                # Save to the cache
                csv_writer.writerow(row)
                # Save to the headers list
                self.headers.append(row)
            cache_file.close()
        else:
            self.log.debug("[%s] => Loading cached data" % self.dataset)
            with open(cache_file_name, 'rb') as cache_file:
                csv_reader = csv.reader(cache_file, delimiter=';', quotechar='"')
                for row in csv_reader:
                    self.headers.append(row)        

        self.log.info("[%s] => %d results" % (self.dataset, len(self.headers)))
        
    def loadMappings(self, mappingFilesPath, sections=None):
        '''
        Loads all the mapping files present in a given directory. The metadata
        file is used to get the list of the files to load and also the
        additional information such as the predicate name and the prefix
        @input sections filter the sections to load
        '''
        # Read the metadata file
        self.log.info("[%s] Loading mappings" % self.dataset)
        metadata = SafeConfigParser()
        metadata.read(mappingFilesPath + "/metadata.txt")
        for section in metadata.sections():
            if sections != None and section not in sections:
                continue
            data = dict(metadata.items(section))
            data['path'] = mappingFilesPath
            self.log.debug("=> %s" % section)
            mappingsList = MappingsList(data)
            self.mappings[section] = mappingsList
              
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    dataset = config.getURI('cedar',"BRT_1889_02_T1")
    
    # Test
    rulesMaker = RuleMaker(config, dataset, "/tmp/test.ttl")
    rulesMaker.loadMappings("DataDump/mapping") #, ['Sex','MaritalStatus']
    rulesMaker.loadHeaders()
    rulesMaker.process() # ['Sex','MaritalStatus']
