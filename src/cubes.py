#!/usr/bin/python2
import bz2
import sys
    
from rdflib import ConjunctiveGraph, Literal, RDF
from common.configuration import Configuration
from common.sparql import SPARQLWrap
from rdflib.namespace import XSD, RDFS
from rdflib.term import URIRef
import requests

# TODO: If the value is not an int mark the point as being ignored
# TODO: When getting the RDF model from the construct, look for dimensions used
# TODO: Add a nice label for the slice
# TODO: Look at http://docs.python-requests.org/en/latest/user/quickstart/#raw-response-content for directly save query output

class CubeMaker(object):
    def __init__(self, configuration):
        """
        Constructor
        """
        # Keep parameters
        self.conf = configuration

        # Get a logger
        self.log = configuration.getLogger("CubeMaker")
        
        # The URI of the harmonised data set
        self._ds_uri = configuration.getURI('cedar','harmonised-data') 
        
        # This is a list of things that are not dimensions        
        self.no_dim = []
        self.no_dim.append(RDF.type)
        self.no_dim.append(self.conf.getURI('prov','wasDerivedFrom'))
        self.no_dim.append(self.conf.getURI('prov','wasGeneratedBy'))
        self.no_dim.append(self.conf.getURI('prov','used'))
        self.no_dim.append(self.conf.getURI('qb','observation'))
        self.no_dim.append(self.conf.getURI('cedarterms','population'))
            
    def generate_dsd(self, output_file):
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
        
        # Finish describing the slices
        slicestruct_uri = self._ds_uri + '-sliced-by-type-and-year'
        graph.add((slicestruct_uri,RDF.type,self.conf.getURI('qb','SliceKey')))
        graph.add((slicestruct_uri,RDFS.label,Literal("Slice by census type and census year")))
        graph.add((slicestruct_uri,self.conf.getURI('qb','componentProperty'),self.conf.getURI('cedar','censusType')))
        graph.add((slicestruct_uri,self.conf.getURI('qb','componentProperty'),self.conf.getURI('sdmx-dimension','refPeriod')))

        # Get the list of slices and add them
        query = """
        select distinct ?slice from <RELEASE> where {
            ?slice qb:observation [a qb:Observation].
        }"""
        sparql = SPARQLWrap(self.conf)
        results = sparql.run_select(query, {'RELEASE' : self.conf.get_graph_name('release')})
        for result in results:
            sliceURI = URIRef(result['slice']['value'])
            graph.add((self._ds_uri,self.conf.getURI('qb','slice'),sliceURI))
            (census_type,census_year) = sliceURI.split('-')[-1].split('_')
            graph.add((sliceURI,RDF.type,self.conf.getURI('qb','Slice')))
            graph.add((sliceURI,RDFS.label,Literal("Slice for census type/year %s/%s" % (census_type,census_year))))
            graph.add((sliceURI,self.conf.getURI('sdmx-dimension','refPeriod'),Literal(int(census_year))))
            graph.add((sliceURI,self.conf.getURI('cedarterms','censusType'),Literal(census_type)))
            graph.add((sliceURI,self.conf.getURI('qb','sliceStructure'),slicestruct_uri))
            
        # Create a DSD
        dsd = self._ds_uri + '-dsd'
        graph.add((self._ds_uri,self.conf.getURI('qb','structure'),dsd))
        graph.add((dsd,RDF.type,self.conf.getURI('qb','DataStructureDefinition')))
        graph.add((dsd,self.conf.getURI('sdmx-attribute','unitMeasure'),URIRef('http://dbpedia.org/resource/Natural_number')))
        ## dimensions
        order = 1
        ### all the encountered dimensions
        query = """
        select distinct ?dimension from <RELEASE> where {
            ?obs a qb:Observation.
            ?obs ?dimension [].
        }"""
        sparql = SPARQLWrap(self.conf)
        results = sparql.run_select(query, {'RELEASE' : self.conf.get_graph_name('release')})
        for result in results:
            dimURI = URIRef(result['dimension']['value'])
            if dimURI not in self.no_dim:
                dim_uri = dsd + "-dimension-" + str(order)
                graph.add((dim_uri, RDF.type, self.conf.getURI('qb','ComponentSpecification')))
                graph.add((dsd,self.conf.getURI('qb','component'),dim_uri))
                graph.add((dim_uri,self.conf.getURI('qb','dimension'),dimURI))
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
        graph.add((dim_uri,self.conf.getURI('qb','dimension'),self.conf.getURI('cedarterms','censusType')))
        graph.add((dim_uri,self.conf.getURI('qb','order'),Literal(order)))
        graph.add((dim_uri,self.conf.getURI('qb','componentAttachment'),self.conf.getURI('qb','Slice')))
        order = order + 1
        ## measure
        measure_uri = dsd + "-measure"
        graph.add((dsd, self.conf.getURI('qb','component'), measure_uri))
        graph.add((measure_uri, RDF.type, self.conf.getURI('qb','ComponentSpecification')))
        graph.add((measure_uri, self.conf.getURI('qb','measure'), self.conf.getURI('cedarterms', 'population')))
        
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
            out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(output_file, "wb")
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
        
        # Fix the parameters for the SPARQL queries
        query_params = {'SHEET'    : sheet_uri,
                        'SLICE'    : slice_uri,
                        'RAW-DATA' : self.conf.get_graph_name('raw-data'),
                        'RULES'    : self.conf.get_graph_name('rules')}
        
        # Prepare the SPARQL construct
        # FIXME With this construct the observation also get the rdfs:label of the mapping body
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
        for (k,v) in query_params.iteritems():
                query = query.replace(k,v)
        query = self.conf.get_prefixes() + query
        
        # Launch the query and stream back the output to the file
        server = self.conf.get_SPARQL()
        creds = [c.strip() for c in open('credentials-virtuoso.txt')]
        r = requests.post(server, auth=(creds[0], creds[1]), data={'query' : query}, stream=True)
        out = bz2.BZ2File(output_file + '.bz2', 'wb', compresslevel=9) if self.conf.isCompress() else open(output_file, "wb")
        for chunk in r.iter_content(2048):
            out.write(chunk)
        out.close() 
        
        #graph = self.sparql.run_construct(query, query_params)
        #self.conf.bindNamespaces(graph)
        #for t in graph.triples((None, None, None)):
        #    (_,p,_) = t
            # Keep track of the dimensions found
        #    if p not in self.no_dim:
        #        self._dimensions.add(p)    
if __name__ == '__main__':
    # Configuration
    config = Configuration('config.ini')
    
    # Set the name of a data set to test
    # BRT_1889_05_T4-S0
    # BRT_1899_03_T-S0 <- Too big ?
    # BRT_1909_02A1_T1-S1 is broken
    #sheet_uri = config.getURI('cedar', 'BRT_1889_05_T4-S0')
    sheet_uri = config.getURI('cedar', 'VT_1859_01_H1-S6')

    # Test
    cube = CubeMaker(config)
    cube.process(sheet_uri, "/tmp/data.ttl")
    cube.generate_dsd("/tmp/extra.ttl")
    
