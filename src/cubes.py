#!/usr/bin/python2
import bz2
import sys
    
from rdflib import ConjunctiveGraph, Literal, RDF
from common.configuration import Configuration
from common.sparql import SPARQLWrap
from rdflib.namespace import XSD, RDFS
from rdflib.term import URIRef
from multiprocessing import Lock
import logging

# TODO: If the value is not an int mark the point as being ignored
# TODO: When getting the RDF model from the construct, look for dimensions used
# TODO: Add a nice label for the slice

class CubeMaker(object):
    def __init__(self, configuration):
        """
        Constructor
        """
        # Keep parameters
        self.conf = configuration

        # Create a lock
        self.lock = Lock()
        
        # Get a logger
        self.log = configuration.getLogger("CubeMaker")
        
        # Create a wrapper for SPARQL queries
        self.sparql = SPARQLWrap(self.conf)
        
        # The URI of the harmonised data set
        self._ds_uri = configuration.getURI('cedar','harmonized-data') 
        
        # Keep a list of slices
        self._slices = set()

        # Keep a set of all the dimensions encountered
        self._dimensions = set()
        
        # This is a list of things that are not dimensions        
        self.no_dim = []
        self.no_dim.append(RDF.type)
        self.no_dim.append(self.conf.getURI('prov','wasDerivedFrom'))
        self.no_dim.append(self.conf.getURI('prov','wasGeneratedBy'))
        self.no_dim.append(self.conf.getURI('prov','used'))
        self.no_dim.append(self.conf.getURI('qb','observation'))
        self.no_dim.append(self.conf.getURI('cedarterms','population'))
        
    
    def _add_slice(self, slice_uri):
        '''
        Register a new slice. Use a lock to support threaded calls
        '''
        with self.lock:
            self._slices.add(slice_uri)
            
    def save_data(self, output_file):
        '''
        Save all additional files into ttl files. Contains data that span
        over all the processed raw cubes
        '''
        # The graph that will be used to store the cube
        graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)
        
        # Create the data set
        graph.add((self._ds_uri,RDF.type,self.conf.getURI('qb','DataSet')))
        graph.add((self._ds_uri,RDF.type,self.conf.getURI('prov','Entity')))
        graph.add((self._ds_uri,self.conf.getURI('dcterms','title'),Literal("Harmonised census data 1795-1971")))
        graph.add((self._ds_uri,self.conf.getURI('rdfs','label'),Literal("Harmonised census data 1795-1971")))
        for s in self._slices:
            graph.add((self._ds_uri,self.conf.getURI('qb','slice'),s))
            
        # Finish describing the slices
        slicestruct_uri = self._ds_uri + '-sliced-by-type-and-year'
        graph.add((slicestruct_uri,RDF.type,self.conf.getURI('qb','SliceKey')))
        graph.add((slicestruct_uri,RDFS.label,Literal("Slice by census type and census year")))
        graph.add((slicestruct_uri,self.conf.getURI('qb','componentProperty'),self.conf.getURI('cedar','censusType')))
        graph.add((slicestruct_uri,self.conf.getURI('qb','componentProperty'),self.conf.getURI('sdmx-dimension','refPeriod')))
        for s in self._slices:
            (census_type,census_year) = s.split('-')[-1].split('_')
            graph.add((s,RDF.type,self.conf.getURI('qb','Slice')))
            graph.add((s,self.conf.getURI('sdmx-dimension','refPeriod'),Literal(int(census_year))))
            graph.add((s,self.conf.getURI('cedar','censusType'),Literal(census_type)))
            graph.add((s,self.conf.getURI('qb','sliceStructure'),slicestruct_uri))

        # Create a DSD
        dsd = self._ds_uri + '-dsd'
        graph.add((self._ds_uri,self.conf.getURI('qb','structure'),dsd))
        graph.add((dsd,RDF.type,self.conf.getURI('qb','DataStructureDefinition')))
        graph.add((dsd,self.conf.getURI('sdmx-attribute','unitMeasure'),URIRef('http://dbpedia.org/resource/Natural_number')))
        ## dimensions
        ### all the encountered dimensions
        order = 1
        for dim in self._dimensions:
            dim_uri = dsd + "-dimension-" + str(order)
            graph.add((dim_uri, RDF.type, self.conf.getURI('qb','ComponentSpecification')))
            graph.add((dsd,self.conf.getURI('qb','component'),dim_uri))
            graph.add((dim_uri,self.conf.getURI('qb','dimension'),dim))
            graph.add((dim_uri,self.conf.getURI('qb','order'),Literal(order)))
            order = order + 1
        ### the ref period used in the slices
        dim_uri = dsd + "-dimension-" + str(order)
        graph.add((dim_uri, RDF.type, self.conf.getURI('qb','ComponentSpecification')))
        graph.add((dsd,self.conf.getURI('qb','component'),dim_uri))
        graph.add((dim_uri,self.conf.getURI('qb','dimension'),self.conf.getURI('sdmx-dimension','refPeriod')))
        graph.add((dim_uri,self.conf.getURI('qb','order'),Literal(order)))
        graph.add((dim_uri,self.conf.getURI('qb','componentAttachment'),self.conf.getURI('qb','Slice')))
        order = order + 1
        ### the census type used in the slices
        dim_uri = dsd + "-dimension-" + str(order)
        graph.add((dim_uri, RDF.type, self.conf.getURI('qb','ComponentSpecification')))
        graph.add((dsd,self.conf.getURI('qb','component'),dim_uri))
        graph.add((dim_uri,self.conf.getURI('qb','dimension'),self.conf.getURI('cedar','censusType')))
        graph.add((dim_uri,self.conf.getURI('qb','order'),Literal(order)))
        graph.add((dim_uri,self.conf.getURI('qb','componentAttachment'),self.conf.getURI('qb','Slice')))
        order = order + 1
        ## measure
        measure_uri = dsd + "-measure"
        graph.add((dsd, self.conf.getURI('qb','component'), measure_uri))
        graph.add((measure_uri, RDF.type, self.conf.getURI('qb','ComponentSpecification')))
        graph.add((measure_uri, self.conf.getURI('qb','measure'), self.conf.getURI('cedar', 'population')))
        
        ## attributes
        attr_uri = dsd + "-attribute"
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
                   self.conf.getURI('qb','DataSet')))
        ## slice key
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
        
    def process(self, sheet_uri, output_file):        
        """
        Process all the data cells in the target sheet and look for rules to
        harmonise them, save the output into outputfile_name
        """
        # Get the name of the slice to use for these observations
        key = '_'.join(sheet_uri.split('/')[-1].split('_')[:2]) 
        slice_uri = self._ds_uri + '-slice-' + key
        self._add_slice(slice_uri)
        
        # Fix the parameters for the SPARQL queries
        query_params = {'SHEET'    : sheet_uri,
                        'SLICE'    : slice_uri,
                        'RAW-DATA' : self.conf.get_graph_name('raw-data'),
                        'RULES'    : self.conf.get_graph_name('rules')}
        
        # Execute the SPARQL construct
        logging.basicConfig(level=logging.DEBUG)
        query = """
        CONSTRUCT {
            <SLICE> qb:observation `iri(bif:concat(?cell,"-h"))`.
            `iri(bif:concat(?cell,"-h"))` a qb:Observation;
                cedarterms:population ?popcount;
                ?dim ?val;
                prov:wasDerivedFrom ?cell;
                prov:wasGeneratedBy `iri(bif:concat(?cell,"-activity"))`.
            `iri(bif:concat(?cell,"-activity"))` a prov:Activity;
                rdfs:label "Harmonise";
                prov:used ?mapping.
        } 
        FROM <RAW-DATA>
        FROM <RULES>
        WHERE {
            ?cell a tablink:DataCell.
            ?cell tablink:sheet <SHEET>.
            ?cell tablink:dimension ?dimension.
            ?cell tablink:value ?popcounts.
            ?mapping a oa:Annotation.
            ?mapping oa:hasTarget ?dimension.
            ?mapping oa:hasBody ?mapping_body.
            ?mapping_body ?dim ?val.
            FILTER (?dim != rdf:type)
            BIND (xsd:decimal(?popcounts) as ?popcount) 
        }"""
        graph = self.sparql.run_construct(query, query_params)
        #graph = ConjunctiveGraph()
        self.conf.bindNamespaces(graph)
        for t in graph.triples((None, None, None)):
            (_,p,_) = t
            # Keep track of the dimensions found
            if p not in self.no_dim:
                self._dimensions.add(p)
                
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
    
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    # Set the name of a data set to test
    # BRT_1889_05_T4_S0 -> 43 obs
    # BRT_1889_05_T5_S0 -> 221 obs
    # VT_1947_A1_T
    # BRT_1889_05_T4_S0
    # BRT_1889_03_T1_S0 <- Huge one! (1163477 triples)
    sheet_uri = config.getURI('cedar', 'BRT_1889_05_T4-S0')

    # Test
    cube = CubeMaker(config)
    cube.process(sheet_uri, "/tmp/data.ttl")
    cube.save_data("/tmp/extra.ttl")
    
