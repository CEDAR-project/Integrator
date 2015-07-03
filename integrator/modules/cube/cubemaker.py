import bz2
import sys
import requests
    
from util.sparql import SPARQLWrap

from rdflib import ConjunctiveGraph, Literal
from rdflib.namespace import XSD, RDFS, RDF, DCTERMS, Namespace
from rdflib.term import URIRef

# TODO: If the value is not an int mark the point as being ignored
# TODO: When getting the RDF model from the construct, look for dimensions used
# TODO: Add a nice label for the slice
# TODO: Look at http://docs.python-requests.org/en/latest/user/quickstart/#raw-response-content for directly save query output

# Define the logger
import logging
from modules.tablinker.namespace import PROV
from modules.cube.namespace import QB, SDMXDIMENSION, SDMXATTRIBUTE
log = logging.getLogger(__name__)


class CubeMaker(object):
    def __init__(self):
        """
        Constructor
        """
        # Set a default namespace
        self.data_ns = Namespace("http://example.org/")
        
        # Compress output by default
        self.compress_output = True
        
        # The URI of the harmonised data set
        #self._ds_uri = configuration.getURI('cedar', 'harmonised-data') 
        
        # This is a list of things that are not dimensions        
        self.no_dim = []
        self.no_dim.append(RDF.type)
        self.no_dim.append(PROV.wasDerivedFrom)
        self.no_dim.append(PROV.wasGeneratedBy)
        self.no_dim.append(PROV.used)
        self.no_dim.append(QB.observation)
        #self.no_dim.append(self.conf.getURI('cedarterms', 'population'))
            
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
        
    def generate_dsd(self, end_point, release_graph_name, output_file):
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
       
        # Create the data set
        ds_uri = self.data_ns['harmonised-cube']
        graph.add((ds_uri, RDF.type, QB.DataSet))
        graph.add((ds_uri, RDF.type, PROV.Entity))
        #graph.add((ds_uri, DCTERMS.title, Literal("Harmonised census data 1795-1971")))
        #graph.add((ds_uri, RDFS.label, Literal("Harmonised census data 1795-1971")))
        
        # Finish describing the slices
        #slicestruct_uri = ds_uri + '-sliced-by-type-and-year'
        #graph.add((slicestruct_uri, RDF.type, QB.SliceKey))
        #graph.add((slicestruct_uri, RDFS.label, Literal("Slice by census type and census year")))
        #graph.add((slicestruct_uri, QB.componentProperty, self.conf.getURI('cedar', 'censusType')))
        #graph.add((slicestruct_uri, QB.componentProperty, SDMXDIMENSION.refPeriod))

        # Get the list of slices and add them
        #query = """
        #prefix qb: <http://purl.org/linked-data/cube#>
        #select distinct ?slice from RELEASE where {
        #    ?slice qb:observation [a qb:Observation].
        #}"""
        #sparql = SPARQLWrap(end_point)
        #results = sparql.run_select(query, {'RELEASE' : release_graph_name})
        #for result in results:
        #    sliceURI = URIRef(result['slice']['value'])
        #    graph.add((ds_uri, QB.slice, sliceURI))
        #    (census_type, census_year) = sliceURI.split('-')[-1].split('_')
        #    graph.add((sliceURI, RDF.type, QB.Slice))
        #    graph.add((sliceURI, RDFS.label, Literal("Slice for census type/year %s/%s" % (census_type, census_year))))
        #    graph.add((sliceURI, SDMXDIMENSION.refPeriod, Literal(int(census_year))))
        #    graph.add((sliceURI, self.conf.getURI('cedarterms', 'censusType'), Literal(census_type)))
        #    graph.add((sliceURI, QB.sliceStructure, slicestruct_uri))
            
        # Create a DSD
        dsd_uri = ds_uri + '-dsd'
        graph.add((ds_uri, QB.structure, dsd_uri))
        graph.add((dsd_uri, RDF.type, QB.DataStructureDefinition))
        graph.add((dsd_uri, SDMXATTRIBUTE.unitMeasure, URIRef('http://dbpedia.org/resource/Natural_number')))
        # # dimensions
        order = 1
        # ## all the encountered dimensions
        query = """
        prefix qb: <http://purl.org/linked-data/cube#>
        select distinct ?dimension from RELEASE where {
            ?obs a qb:Observation.
            ?obs ?dimension [].
        }"""
        sparql = SPARQLWrap(end_point)
        results = sparql.run_select(query, {'RELEASE' : release_graph_name})
        for result in results:
            dimURI = URIRef(result['dimension']['value'])
            if dimURI not in self.no_dim:
                dim_uri = dsd_uri + "-dimension-" + str(order)
                graph.add((dim_uri, RDF.type, QB.ComponentSpecification))
                graph.add((dsd_uri, QB.component, dim_uri))
                graph.add((dim_uri, QB.dimension, dimURI))
                graph.add((dim_uri, QB.order, Literal(order)))
                order = order + 1
        # ## the ref period used in the slices
        #dim_uri = dsd_uri + "-dimension-" + str(order)
        #graph.add((dim_uri, RDF.type, QB.ComponentSpecification))
        #graph.add((dsd_uri, QB.component, dim_uri))
        #graph.add((dim_uri, QB.dimension, SDMXDIMENSION.refPeriod))
        #graph.add((dim_uri, QB.order, Literal(order)))
        #graph.add((dim_uri, QB.componentAttachment, QB.Slice))
        #order = order + 1
        # ## the census type used in the slices
        #dim_uri = dsd_uri + "-dimension-" + str(order)
        #graph.add((dim_uri, RDF.type, QB.ComponentSpecification))
        #graph.add((dsd_uri, QB.component, dim_uri))
        #graph.add((dim_uri, QB.dimension, self.conf.getURI('cedarterms', 'censusType')))
        #graph.add((dim_uri, QB.order, Literal(order)))
        #graph.add((dim_uri, QB.componentAttachment, QB.Slice))
        #order = order + 1
        # # measure
        measure_uri = dsd_uri + "-measure"
        graph.add((dsd_uri, QB.component, measure_uri))
        graph.add((measure_uri, RDF.type, QB.ComponentSpecification))
        #graph.add((measure_uri, QB.measure, self.conf.getURI('cedarterms', 'population')))
        
        # # attributes
        attr_uri = dsd_uri + "-attribute"
        graph.add((dsd_uri,QB.component,attr_uri))
        graph.add((attr_uri,
                   QB.attribute,
                   SDMXATTRIBUTE.unitMeasure))
        graph.add((attr_uri,
                   QB.componentRequired,
                   Literal("true", datatype=XSD.boolean)))
        graph.add((attr_uri,QB.componentAttachment,QB.DataSet))
        # # slice key
        #graph.add((dsd,
        #           QB.sliceKey,
        #           slicestruct_uri))
        
        log.info("[{}] Contains {} triples".format(output_file, len(graph)))
        try :
            out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.compress_output else open(output_file, "wb")
            graph.serialize(destination=out, format='n3')
            out.close()
        except :
            log.error("Whoops! Something went wrong in serializing to output file")
            log.info(sys.exc_info())
        
    def process(self, end_point, raw_data_graph, rules_graph, sheet_name, output_file):        
        """
        Process all the data cells in the target sheet and look for rules to
        harmonise them, save the output into outputfile_name
        """
        # Get the name of the slice to use for these observations
        # key = '_'.join(sheet_uri.split('/')[-1].split('_')[:2]) 
        # slice_uri = self._ds_uri + '-slice-' + key
        
        # Fix the parameters for the SPARQL queries
        query_params = {'SHEET'        : self.data_ns[sheet_name],
                        '__RAW_DATA__' : raw_data_graph,
                        '__RULES__'    : rules_graph}
        
        # Prepare the SPARQL construct
        # FIXME With this construct the observation also get the rdfs:label of the mapping body
        # <SLICE> qb:observation `iri(bif:concat(?cell,"-h"))`.
        query = """
        PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX tablinker: <http://bit.ly/cedar-tablink#>
        PREFIX oa: <http://www.w3.org/ns/openannotation/core/>
        
        CONSTRUCT {
            `iri(bif:concat(?cell,"-h"))` a qb:Observation;
                <http://example.org/cedarterms#population> ?popcount;
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
        r = requests.post(end_point, data={'query' : query}, stream=True)
        out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.compress_output else open(output_file, "wb")
        for chunk in r.iter_content(2048):
            out.write(chunk)
        out.close() 
        
