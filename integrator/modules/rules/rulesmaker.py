import bz2
import datetime
import pprint

from rdflib import ConjunctiveGraph, RDF, URIRef
from util.sparql import SPARQLWrap
from rdflib.term import Literal
from rdflib.namespace import RDFS, DCTERMS, XSD, Namespace
from ConfigParser import SafeConfigParser

from modules.tablinker.namespace import PROV, DCAT, OA
from modules.rules.mappings import MappingsList

import sys
reload(sys)
sys.setdefaultencoding("utf8")  # @UndefinedVariable

pp = pprint.PrettyPrinter(indent=2)

HEADERS_QUERY = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
PREFIX tablinker: <http://bit.ly/cedar-tablink#>
PREFIX dcterms: <http://purl.org/dc/terms/>

SELECT DISTINCT ?cell ?literal ?header_type ?dataset_name
FROM __RAW_DATA__
WHERE {
    ?cell a ?header_type.
    ?cell tablinker:value ?literal.
    ?cell tablinker:sheet __DATA_SET__. 
    ?dataset dcterms:hasPart __DATA_SET__.
    ?dataset rdfs:label ?dataset_name.
    FILTER (?header_type IN (tablinker:RowHeader,tablinker:ColumnHeader))
    FILTER (?literal != "")
} 
"""

INTEGRATOR_URI = URIRef("https://github.com/CEDAR-project/Integrator")

import logging
log = logging.getLogger(__name__)

    
class RuleMaker(object):
    def __init__(self, end_point, dataset, output_file_name):
        """
        Constructor
        """
        self.dataset = dataset
        self.output_file_name = output_file_name
        self.mappings = {}
        self.headers = []
        
        # The location of the SPARQL end_point
        self.end_point = end_point

        # Create the graph
        self.graph = ConjunctiveGraph()
        self.graph.bind('rdf', RDF)
        self.graph.bind('rdfs', RDFS)
        self.graph.bind('prov', PROV)
        self.graph.bind('dcat', DCAT)
        self.graph.bind('oa', OA)
        self.graph.bind('dcterms', DCTERMS)
    
        # Set a default namespace
        self.data_ns = Namespace("http://example.org/")
        self.graph.bind('data', self.data_ns)
        
    def set_target_namespace(self, namespace):
        """
        Set the target namespace used to prefix all the resources of the
        data generated
        """
        self.data_ns = Namespace(namespace)
        self.graph.namespace_manager.bind('data', self.data_ns, 
                                          override=True, replace=True)
    
    def set_compress(self, value):
        """
        Set the usage of compression on or off
        """
        self.compress_output = value
        
    def process(self, dims=None):
        '''
        Function used to process one of the sheets in the data sets
        '''
        try:
            if dims == None:
                dims = sorted(self.mappings.keys())
                
            # Mint a URI for the activity
            activity_URI = URIRef(self.dataset + '-mapping-activity')
    
            # Keep the start time
            startTime = self._now()
                
            log.debug("Start processing %s" % dims)
            for dim in dims:
                log.debug("=> %s" % dim)
                count = self._process_mapping(self.graph, activity_URI, dim)
                
                if count != 0:
                    # Describe the file used
                    mappingFileDSURI = activity_URI + '-' + dim 
                    mappingFileDistURI = mappingFileDSURI + '-dist'
                    self.graph.add((activity_URI, PROV.used, mappingFileDSURI))
                    self.graph.add((mappingFileDSURI, RDF.type, DCAT.Dataset))
                    self.graph.add((mappingFileDSURI, RDFS.label, Literal(dim)))
                    self.graph.add((mappingFileDSURI, DCAT.distribution, mappingFileDistURI))
                    self.graph.add((mappingFileDistURI, RDF.type, DCAT.Distribution))
                    self.graph.add((mappingFileDistURI, RDFS.label, Literal(self.mappings[dim].get_file_name())))
                    self.graph.add((mappingFileDistURI, DCTERMS.accessURL, self.mappings[dim].get_src_URI()))
                
            # Keep the end time
            endTime = self._now()
            
            # Finish describing the activity
            self.graph.add((activity_URI, RDF.type, PROV.Activity))
            self.graph.add((activity_URI, RDFS.label, Literal("Annotate")))
            self.graph.add((activity_URI, PROV.startedAtTime, startTime))
            self.graph.add((activity_URI, PROV.endedAtTime, endTime))
            self.graph.add((activity_URI, PROV.wasAssociatedWith, INTEGRATOR_URI))
    
            # Save the graph
            log.info("[{}] Saving {} data triples.".format(self.dataset, len(self.graph)))
            try :
                out = bz2.BZ2File(self.output_file_name + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(self.output_file_name, "w")
                self.graph.serialize(destination=out, format='n3')
                out.close()
            except :
                log.error("[{}] Whoops! Something went wrong in serialising to output file".format(self.dataset))
        
        except:
            log.error("[{}] Something bad happened: {}".format(self.dataset, sys.exc_info()[0]))
            
    def _process_mapping(self, graph, activity_URI, dimension_name):
        count = 0
        
        # Process all the headers one by one
        mappings_list = self.mappings[dimension_name]
        for header in self.headers:
            [_, literal, _, cell_name, sheet_name, dataset_name] = header
            context_map = {'cell' : cell_name,
                           'sheet': sheet_name,
                           'dataset' : dataset_name}
            pairs = mappings_list.get_mappings_for(literal, context_map)
            cell_uri = self.data_ns[cell_name]
            if pairs != None:
                count = count + 1
                # Mint URIs
                annotation_URI = URIRef(cell_uri + "-mapping")
                annotation_body_URI = annotation_URI + "-body"
                # Add the triples
                graph.add((annotation_URI, RDF.type, OA.Annotation))
                graph.add((annotation_URI, RDFS.label, Literal('Mapping')))
                graph.add((annotation_URI, OA.hasBody, annotation_body_URI))
                graph.add((annotation_URI, OA.hasTarget, cell_uri))
                graph.add((annotation_URI, OA.serializedAt, self._now()))
                graph.add((annotation_URI, OA.serializedBy, INTEGRATOR_URI))
                graph.add((annotation_URI, PROV.wasGeneratedBy, activity_URI))
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
                       datatype=XSD.dateTime)
        
    def loadHeaders(self, graph_name):
        '''
        This method fetches all the header used in the raw data and
        saves them as a cache in a CSV file
        '''
        log.info("[{}] Loading headers".format(self.dataset))
        
        # Load and execute the SPARQL query, save to the cache too
        sparql = SPARQLWrap(self.end_point)
        sparql_params = {'__DATA_SET__' : self.data_ns[self.dataset].n3(),
                         '__RAW_DATA__' : graph_name}
        log.info(self.end_point)
        log.info(sparql_params)
        results = sparql.run_select(HEADERS_QUERY, sparql_params)
        for result in results:
            # Parse the result
            cell = result['cell']['value']
            cell_name = cell.split('/')[-1]
            header_type = result['header_type']['value']
            dataset_name = result['dataset_name']['value']
            sheet_name = self.dataset.split('/')[-1] 
            literal = result['literal']['value']
            row = [cell_name, literal, header_type, cell_name, sheet_name, dataset_name]
            # Save to the headers list
            self.headers.append(row)

        log.info("[%s] => %d results" % (self.dataset, len(self.headers)))
        
    def loadMappings(self, mappingFilesPath, sections=None):
        '''
        Loads all the mapping files present in a given directory. The metadata
        file is used to get the list of the files to load and also the
        additional information such as the predicate name and the prefix
        @input sections filter the sections to load
        '''
        # Read the metadata file
        log.info("[%s] Loading mappings" % self.dataset)
        metadata = SafeConfigParser()
        metadata.read(mappingFilesPath + "/metadata.txt")
        for section in metadata.sections():
            if sections != None and section not in sections:
                continue
            try:
                log.debug("=> %s" % section)
                data = dict(metadata.items(section))
                data['path'] = mappingFilesPath
                mappingsList = MappingsList(data, self.conf)
                self.mappings[section] = mappingsList
            except:
                log.error("[{}] Something bad happened with {} : {}".format(self.dataset, section, sys.exc_info()[0]))
              
