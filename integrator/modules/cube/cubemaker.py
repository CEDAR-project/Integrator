import bz2
import sys
import requests
    
from util.sparql import SPARQLWrap

from rdflib import ConjunctiveGraph, Literal
from rdflib.namespace import XSD, RDFS, RDF, DCTERMS, Namespace
from rdflib.term import URIRef, BNode
from modules.tablinker.namespace import PROV
from modules.cube.namespace import QB, SDMXDIMENSION, SDMXATTRIBUTE

# TODO: If the value is not an int mark the point as being ignored
# TODO: When getting the RDF model from the construct, look for dimensions used
# TODO: Add a nice label for the slice
# TODO: Look at http://docs.python-requests.org/en/latest/user/quickstart/#raw-response-content for directly save query output

# Define the logger
import logging
log = logging.getLogger(__name__)

# This query is used to get a list of all the dimensions associated to the
# observations present in the release dataset
QUERY_DIMS = """
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?dim FROM __RELEASE__ WHERE {
    ?obs a qb:Observation.
    ?obs ?dim [].
    FILTER (?dim NOT IN (rdfs:label, rdf:type, prov:wasDerivedFrom, prov:wasGeneratedFrom))
}
"""

# This query returns a list of observations associated to a particular source
# file. This is used to bind the observations of the slices defined for those
# source files
QUERY_MEMBER_OBS = """
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX tablinker: <http://bit.ly/cedar-tablink#>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
SELECT DISTINCT ?obs FROM __RELEASE__ FROM __RAW_DATA__ WHERE {
    ?obs a qb:Observation.
    ?obs prov:wasDerivedFrom ?cell.
    ?cell a tablinker:DataCell.
    ?cell tablinker:sheet ?sheet.
    ?ds a dcat:DataSet.
    ?ds dcterms:hasPart ?sheet.
    ?ds prov:wasDerivedFrom ?source.
    ?source dcat:distribution [rdfs:label ?name]
    FILTER (?name IN (__SOURCES__))
}
"""

class CubeMaker(object):
    def __init__(self, end_point, release_graph_name, raw_data_graph_name, rules_graph_name):
        """
        Constructor
        """
        # Save the parameters
        self.end_point = end_point
        self.release_graph_name = release_graph_name
        self.raw_data_graph_name = raw_data_graph_name
        self.rules_graph_name = rules_graph_name
        
        # Set a default namespace
        self.data_ns = Namespace("http://example.org/")
        
        # Compress output by default
        self.compress_output = True
        
    def set_target_namespace(self, namespace):
        """
        Set the target namespace used to prefix all the resources of the
        data generated
        """
        self.data_ns = Namespace(namespace)
    
    def set_compress(self, value):
        """
        Set the usage of compression on or off
        """
        self.compress_output = value
        
    def generate_dsd(self, title, measure, measure_unit, slices, output_file):
        '''
        Save all additional files into ttl files. Contains data that span
        over all the processed raw cubes
        '''
        # The graph that will be used to store the cube
        graph = ConjunctiveGraph()
        graph.bind('prov', PROV)
        graph.bind('dcterms', DCTERMS)
        graph.bind('qb', QB)
        graph.bind('sdmx-dimension', SDMXDIMENSION)
        graph.bind('sdmx-attribute', SDMXATTRIBUTE)
        graph.bind('data', self.data_ns)
       
        # Create the data set description
        ds_uri = self.data_ns['harmonised-cube']
        graph.add((ds_uri, RDF.type, QB.DataSet))
        graph.add((ds_uri, RDF.type, PROV.Entity))
        graph.add((ds_uri, DCTERMS.title, Literal(title)))
        graph.add((ds_uri, RDFS.label, Literal(title)))
        
        # Create the DSD
        dsd_uri = ds_uri + '-dsd'
        graph.add((ds_uri, QB.structure, dsd_uri))
        graph.add((dsd_uri, RDF.type, QB.DataStructureDefinition))
        graph.add((dsd_uri, SDMXATTRIBUTE.unitMeasure, URIRef(measure_unit)))
        
        # Bind all the dimensions
        sparql = SPARQLWrap(self.end_point)
        params = {'__RELEASE__' : self.release_graph_name}
        results = sparql.run_select(QUERY_DIMS, params)
        dims = [URIRef(r['dim']['value']) for r in results]
        if URIRef(measure) in dims:
            dims.remove(URIRef(measure)) # We need to remove the measure
        for index in range(0,len(dims)):
            dim_uri = BNode()
            graph.add((dsd_uri, QB.component, dim_uri))
            graph.add((dim_uri, QB.dimension, dims[index]))
            graph.add((dim_uri, QB.order, Literal(index+1)))
        
        # Bind all the dimensions used in the slices too
        slice_dims = list(set([s['property'] for s in slices]))  
        for index in range(0, len(slice_dims)):
            dim_uri = BNode()
            graph.add((dsd_uri, QB.component, dim_uri))
            graph.add((dim_uri, QB.dimension, URIRef(slice_dims[index])))
            graph.add((dim_uri, QB.order, Literal(len(dims)+index+1)))
            graph.add((dim_uri, QB.componentAttachment, QB.Slice))
        
        # Bind the measure
        measure_uri = BNode()
        graph.add((dsd_uri, QB.component, measure_uri))
        graph.add((measure_uri, QB.measure, URIRef(measure)))
        
        # Bind the attributes
        attr_uri = BNode()
        graph.add((dsd_uri, QB.component, attr_uri))
        graph.add((attr_uri, QB.attribute, SDMXATTRIBUTE.unitMeasure))
        graph.add((attr_uri, QB.componentRequired, Literal("true", datatype=XSD.boolean)))
        graph.add((attr_uri, QB.componentAttachment, QB.DataSet))
        
        # Now create all the slices
        for index in range(0, len(slices)):
            # That's our slice
            s = slices[index]
            
            # Add a slice key to the DSD
            slice_uri = ds_uri + '-slice_' + str(index) 
            slicekey_uri = slice_uri + '-key'
            graph.add((dsd_uri, QB.sliceKey, slicekey_uri))
            graph.add((slicekey_uri, RDF.type, QB.SliceKey))
            graph.add((slicekey_uri, RDFS.label, Literal(s['title'])))
            graph.add((slicekey_uri, QB.componentProperty, URIRef(s['property'])))
            
            # Try to guess the type of the value
            casted_val = s['value']
            try:
                casted_val = int(casted_val)
            except ValueError:
                pass
            val = Literal(casted_val)
            
            # Describe the slice
            graph.add((slice_uri, RDF.type, QB.Slice))
            graph.add((slice_uri, QB.sliceStructure, slicekey_uri))
            graph.add((slice_uri, URIRef(s['property']), val))
        
            # Attach all the relevant observations to it
            sparql = SPARQLWrap(self.end_point)
            s2 = [Literal(s).n3() for s in s['sources']]
            params = {'__RELEASE__' : self.release_graph_name,
                      '__RAW_DATA__': self.raw_data_graph_name,
                      '__SOURCES__' : ','.join(s2)
                      }
            results = sparql.run_select(QUERY_MEMBER_OBS, params)
            for r in results:
                graph.add((slice_uri, QB.observation, URIRef(r['obs']['value'])))
                
        log.info("[{}] Contains {} triples".format(output_file, len(graph)))
        try :
            out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.compress_output else open(output_file, "wb")
            graph.serialize(destination=out, format='n3')
            out.close()
        except :
            log.error("Whoops! Something went wrong in serializing to output file")
            log.info(sys.exc_info())
        
    def process(self, measure, sheet_name, output_file):        
        """
        Process all the data cells in the target sheet and look for rules to
        harmonise them, save the output into outputfile_name
        """
        # Set the parameters for the SPARQL queries
        query_params = {'SHEET'        : self.data_ns[sheet_name],
                        '__RAW_DATA__' : self.raw_data_graph_name,
                        '__RULES__'    : self.rules_graph_name,
                        '__MEASURE__'  : measure}
        
        # Prepare the SPARQL construct
        query = """
        PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX tablinker: <http://bit.ly/cedar-tablink#>
        PREFIX oa: <http://www.w3.org/ns/openannotation/core/>
        
        CONSTRUCT {
            `iri(bif:concat(?cell,"-h"))` a qb:Observation;
                <__MEASURE__> ?popcount;
                ?dim ?val;
                prov:wasDerivedFrom ?cell;
                prov:wasGeneratedBy `iri(bif:concat(?cell,"-activity"))`.
            `iri(bif:concat(?cell,"-activity"))` a prov:Activity;
                rdfs:label "Harmonise";
                prov:used ?mapping.
        } 
        FROM __RAW_DATA__
        FROM __RULES__
        WHERE {
            ?cell a tablinker:DataCell.
            ?cell tablinker:sheet <SHEET>.
            ?cell tablinker:dimension ?dimension.
            ?cell tablinker:value ?popcounts.
            ?mapping a oa:Annotation.
            ?mapping oa:hasTarget ?dimension.
            ?mapping oa:hasBody ?mapping_body.
            ?mapping_body ?dim ?val.
            FILTER (?dim != rdf:type)
            BIND (xsd:decimal(?popcounts) as ?popcount) 
        }"""
        for (k, v) in query_params.iteritems():
                query = query.replace(k, v)
        
        # Launch the query and stream back the output to the file
        r = requests.post(self.end_point, data={'query' : query}, stream=True)
        out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.compress_output else open(output_file, "wb")
        for chunk in r.iter_content(2048):
            out.write(chunk)
        out.close() 
        
