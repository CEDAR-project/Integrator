#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef
import re
import uuid
import bz2

class RuleMaker(object):
    namespaces = {
      'dcterms':Namespace('http://purl.org/dc/terms/'), 
      'skos':Namespace('http://www.w3.org/2004/02/skos/core#'), 
      'tablink':Namespace('http://example.org/ns#'), 
      'harmonizer':Namespace('http://harmonizer.example.org/ns#'),
      'rules':Namespace('http://rules.example.org/resource/'),
      'qb':Namespace('http://purl.org/linked-data/cube#'), 
      'owl':Namespace('http://www.w3.org/2002/07/owl#'),
      'sdmx-dimension':Namespace('http://purl.org/linked-data/sdmx/2009/dimension#'),
      'sdmx-code':Namespace('http://purl.org/linked-data/sdmx/2009/code#')
    }
    
    def __init__(self, tablename, endpoint, namedgraph):
        """
        Constructor
        """
        # Keep parameters
        self.tablename = tablename
        self.endpoint = endpoint
        self.namedgraph = namedgraph
        
        # The graph that will be used to store the rules
        self.graph = ConjunctiveGraph()
        for namespace in self.namespaces:
            self.graph.namespace_manager.bind(namespace, self.namespaces[namespace])
        
    def go(self):
        """
        Start the job
        """
        
        # Get a list of all the column headers
        headers = {}
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?header ?label ?parent from <GRAPH> where {
        ?header a <http://example.org/ns#ColumnHeader>.
        ?header <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
        ?header <http://example.org/ns#subColHeaderOf> ?parent.
        }
        """.replace('GRAPH',namedgraph)
        print query
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            resource = result['header']['value']
            label = result['label']['value']
            parent = result['parent']['value']
            headers[resource] = {}
            headers[resource]['label'] = label
            headers[resource]['parent'] = parent
         
        # Detect sex
        self.detect_sex(headers)
        
        # Detect total
        self.detect_sums(headers)
    
    def saveTo(self, filename):
        """
        Write the file to disk
        """
        file = bz2.BZ2File(filename, 'wb', compresslevel=9)
        turtle = self.graph.serialize(destination=None, format='turtle')
        file.writelines(turtle)
        file.close()
        
    def detect_sex(self, headers):
        """
        Look for columns indicating the sex of individuals
        """
        for (resource, content) in headers.iteritems():
            label_clean = self._clean_string(content['label'])
            
            # Women
            if label_clean == 'v' or label_clean == 'vrouwen' or label_clean == 'vrouwelijk geslacht':
                self.create_rule_add_dimension(URIRef(resource),
                                               self.namespaces['sdmx-dimension']['sex'],
                                               self.namespaces['sdmx-code']['sex-V'])

            # Man            
            if label_clean == 'm' or label_clean == 'mannen' or label_clean == 'mannelijk geslacht':
                self.create_rule_add_dimension(URIRef(resource),
                                               self.namespaces['sdmx-dimension']['sex'],
                                               self.namespaces['sdmx-code']['sex-M'])
    
    def detect_sums(self, headers):
        """
        Look for columns used for totals
        """
        pass
    
    def _clean_string(self, text):
        """
        Utility function to clean a string
        """
        text_clean = text.replace('.', '').replace('_', ' ').lower()
        text_clean = re.sub(r'\s+', ' ', text_clean)
        return text_clean
    
    def create_rule_add_dimension(self, target, dimension, value):
        """
        Create a new harmonization rule that assign a dimension and value
        to all the observations having the target as a dimension
        """
        resource = URIRef(self.namespaces['rules'] + str(uuid.uuid1()))
        self.graph.add((resource,
                        RDF.type,
                        self.namespaces['harmonizer']['AddDimensionRule']))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['target'],
                        target))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['dimension'],
                        dimension))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['value'],
                        value))
    
if __name__ == '__main__':
    #table = 'BRT_1899_10_T_marked'
    tables = [table.strip() for table in open('tables.txt')]
    for table in tables:
        namedgraph = 'http://example.com/graph/TABLE'.replace('TABLE', table)
        r = RuleMaker(table, 'http://127.0.0.1:1234/sparql/', namedgraph)
        r.go()
        r.saveTo('rules/' + table + '.ttl.bz2')
        