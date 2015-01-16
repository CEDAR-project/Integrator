#!/usr/bin/python2
import bz2
import datetime
import os
import pprint

from rdflib import ConjunctiveGraph, RDF, URIRef
from common.configuration import Configuration
from common.sparql import SPARQLWrap
from rdflib.term import Literal
from rdflib.namespace import RDFS
from ConfigParser import SafeConfigParser
from xlrd import open_workbook
from xlutils.margins import number_of_good_cols, number_of_good_rows
import requests

pp = pprint.PrettyPrinter(indent=2)

# TODO add rdfs:label to everything

class MappingsList(object):
    
    def __init__(self, data):
        self._mappings = {}
        
        self.excelFileName = data['file']
        predicate = URIRef(data['predicate'])
        prefix = data['prefix']
        
        # Load the mappings
        wb = open_workbook(data['path'] + "/" + self.excelFileName, formatting_info=False, on_demand=True)
        sheet = wb.sheet_by_index(0)
        colns = number_of_good_cols(sheet)
        rowns = number_of_good_rows(sheet)
        for i in range(1, rowns):
            # Do we have a specific target ?
            target = sheet.cell(i, 0).value
            if target == '':
                target = 'Any'
            
            # Get the string
            literal = Literal(sheet.cell(i, 1).value)
            
            # Get the values
            values = []
            for j in range(2, colns):
                value = sheet.cell(i, j).value
                if type(value) == type(1.0):
                    value = str(int(value))
                if value != '':
                    pair = None
                    if value == 'total':
                        pair = (URIRef("http://bit.ly/cedar#isTotal"), Literal("1"))                        
                    else:
                        pair = (predicate, URIRef(prefix + value))
                    values.append(pair)
                    
            # Save the mapping
            if len(values) > 0:
                self._mappings.setdefault(target, {})
                self._mappings[target][literal] = values
        
    def get_src_URI(self):
        """
        Return a URI for the Excel file
        """
        root = URIRef("https://raw.githubusercontent.com/CEDAR-project/DataDump/master/mapping/")
        mappingsDumpURI = root + os.path.relpath(self.excelFileName)
        return URIRef(mappingsDumpURI)
    
    def get_file_name(self):
        return self.excelFileName
    
    def get_mappings(self):
        return self._mappings
    
class RuleMaker(object):
    def __init__(self, configuration, mappingFilesPath, outputPath):
        """
        Constructor
        """
        self.conf = configuration
        self.log = self.conf.getLogger("RuleMaker")
        self.outputPath = outputPath
        self.mappings = {}
        
        # Read the metadata file
        self.log.info("Loading mappings")
        metadata = SafeConfigParser()
        metadata.read(mappingFilesPath + "/metadata.txt")
        for section in metadata.sections():
            data = dict(metadata.items(section))
            data['path'] = mappingFilesPath
            self.log.info("=> %s" % section)
            mappingsList = MappingsList(data)
            self.mappings[section] = mappingsList
            
        # Create a wrapper for SPARQL queries
        self.sparql = SPARQLWrap(self.conf)

    def process(self, dims=None):
        '''
        Function used to process one of the sheets in the data sets
        '''
        if dims == None:
            dims = self.mappings.keys()
            
        self.log.info("Start processing %s" % dims)
        for dim in dims:
            self.log.info("=> %s" % dim)
            self._process_mapping(dim)
            
    def _process_mapping(self, dimension_name):
        output_file = self.outputPath + "/" + dimension_name + '.nt'
        out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(output_file, "wb")
        
        mappings_list = self.mappings[dimension_name]
        
        # Mint a URI for the activity
        dataset_uri = self.conf.getURI('cedar', 'raw-rdf') 
        activity_URI = URIRef(dataset_uri + '-' + dimension_name + '-mapping-activity')
        
        # Keep the start time
        startTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            datatype=self.conf.getURI('xsd', 'dateTime'))
                
        for (scope, mappings_pairs) in mappings_list.get_mappings().iteritems():
            for (literal, pairs) in mappings_pairs.iteritems():
                self._process_mapping_entry(activity_URI, scope, literal, pairs, out)
            
        # Keep the end time
        endTime = Literal(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                            datatype=self.conf.getURI('xsd', 'dateTime'))
        
        graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)
        
        # Finish describing the activity
        graph.add((activity_URI, RDF.type, self.conf.getURI('prov', 'Activity')))
        graph.add((activity_URI, RDFS.label, Literal("Annotate for " + dimension_name)))
        graph.add((activity_URI, self.conf.getURI('prov', 'startedAtTime'), startTime))
        graph.add((activity_URI, self.conf.getURI('prov', 'endedAtTime'), endTime))
        graph.add((activity_URI, self.conf.getURI('prov', 'wasAssociatedWith'), URIRef("https://github.com/CEDAR-project/Integrator")))
        mappingFileDistURI = URIRef(dataset_uri + '-' + dimension_name + '-dist')
        graph.add((activity_URI, self.conf.getURI('prov', 'used'), mappingFileDistURI))
        graph.add((mappingFileDistURI, RDF.type, self.conf.getURI('dcat', 'Distribution')))
        graph.add((mappingFileDistURI, RDFS.label, Literal(mappings_list.get_file_name())))
        graph.add((mappingFileDistURI, self.conf.getURI('dcterms', 'accessURL'), mappings_list.get_src_URI()))
        graph.serialize(destination=out, format='nt')
        out.close() 

    def _process_mapping_entry(self, activity_URI, scope, literal, pairs, out):
        self.log.debug("==> %s" % literal.n3())
        query = """
        CONSTRUCT {
            `iri(bif:concat(?cell,"-mapping"))` a oa:Annotation;
                rdfs:label "Mapping";
                oa:hasBody `iri(bif:concat(?cell,"-mapping-body"))`;
                oa:hasTarget ?cell;
                oa:serializedAt ?date;
                oa:serializedBy <https://github.com/CEDAR-project/Integrator> ;
                prov:wasGeneratedBy __MAPPING_ACTIVITY__ .
            `iri(bif:concat(?cell,"-mapping-body"))` a rdfs:Resource;
                rdfs:label "Mapping body";
                __PAIRS__
        } 
        FROM <RAW-DATA>
        WHERE {
            ?cell a ?header.
            ?cell tablink:value __LITERAL__.
            __SCOPE__
            FILTER (?header in (tablink:RowHeader,tablink:ColumnHeader))
            BIND (now() AS ?date)
        }"""
        # Set the graph name
        query = query.replace('RAW-DATA', self.conf.get_graph_name('raw-data'))
        
        # Set the activity
        query = query.replace('__MAPPING_ACTIVITY__', activity_URI.n3())
        
        # Set the scope
        scope_str = ''
        if scope != 'Any':
            scope_str = """
                ?cell tablink:sheet ?sheet. 
                ?ds dcterms:hasPart ?sheet.
                ?ds rdfs:label \"%s\". """ % scope
        query = query.replace('__SCOPE__', scope_str)
        
        # Set the literal
        query = query.replace('__LITERAL__', literal.n3())
        
        # Set the pairs
        pairs_txt = ""
        for index in range(0, len(pairs)):
            (p, o) = pairs[index]
            pairs_txt = pairs_txt + p.n3() + " " + o.n3()
            if index == len(pairs) - 1:
                pairs_txt = pairs_txt + "."
            else:
                pairs_txt = pairs_txt + ";"
        query = query.replace('__PAIRS__', pairs_txt)
        
        # Add the namespaces
        query = self.conf.get_prefixes() + query
        
        # Launch the query and stream back the output to the file
        server = self.conf.get_SPARQL()
        r = requests.post(server, auth=('rdfread', 'red_fred'), data={'query' : query, 'format':'text/plain'}, stream=True)
        for chunk in r.iter_content(2048):
            out.write(chunk)
                
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    # Test
    rulesMaker = RuleMaker(config, "DataDump/mapping", "/tmp")
    rulesMaker.process() # ['Sex']
