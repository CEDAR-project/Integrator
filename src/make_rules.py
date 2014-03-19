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
        max_depth = 0
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?header ?label ?parent ?depth from <GRAPH> where {
        ?header a <http://example.org/ns#ColumnHeader>.
        ?header <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
        ?header <http://example.org/ns#subColHeaderOf> ?parent.
        ?header <http://example.org/ns#depth> ?depth.
        }
        """.replace('GRAPH',namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            resource = result['header']['value']
            headers[resource] = {}
            headers[resource]['label'] = result['label']['value']
            headers[resource]['parent'] = result['parent']['value']
            headers[resource]['depth'] = int(result['depth']['value'])
            if headers[resource]['depth'] > max_depth:
                max_depth = headers[resource]['depth']
        
        # Process all the leaf headers, one by one
        for (header, data) in headers.iteritems():
            # Skip non leaf
            if data['depth'] != max_depth:
                continue
            self.process_header(headers, header, header)
            
    
    def process_header(self, headers, leaf, header):
        # Get the data
        data = headers[header]
        
        # Detect several things in the label
        self.detect_sex(leaf, data['label'])
        self.detect_sums(leaf, data['label'])
            
        # Detect total
        #self.detect_sums(headers)
        
        # Recurse (added filter to go around vertical merge)
        if data['depth'] != 1:
            if headers[data['parent']]['label'] != data['label']:
                self.process_header(headers, leaf, data['parent'])
    
    def saveTo(self, filename):
        """
        Write the file to disk
        """
        file = bz2.BZ2File(filename, 'wb', compresslevel=9)
        turtle = self.graph.serialize(destination=None, format='turtle')
        file.writelines(turtle)
        file.close()
        
    def detect_sex(self, resource, label):
        """
        Check for known labels for sex
        """
        label_clean = self._clean_string(label)
        
        # Women
        if label_clean == 'v' or label_clean == 'vrouwen' or label_clean == 'vrouwelijk geslacht':
            self.create_rule_add_dimension(URIRef(resource),
                                           self.namespaces['sdmx-dimension']['sex'],
                                           self.namespaces['sdmx-code']['sex-V'])
            return True

        # Man            
        if label_clean == 'm' or label_clean == 'mannen' or label_clean == 'mannelijk geslacht':
            self.create_rule_add_dimension(URIRef(resource),
                                           self.namespaces['sdmx-dimension']['sex'],
                                           self.namespaces['sdmx-code']['sex-M'])
            return True
        return False
    
    def detect_sums(self, resource, label):
        """
        Check if the label speaks about "totaal"
        """
        return False
    
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
        t = table.split('_')
        type = t[0]
        year = t[1]
        if type != 'BRT' or year != '1920':
            continue
        print table
        namedgraph = 'http://lod.cedar-project.nl/resource/v2/TABLE'.replace('TABLE', table)
        r = RuleMaker(table, 'http://lod.cedar-project.nl:8080/sparql/cedar', namedgraph)
        r.go()
        r.saveTo('rules/' + table + '.ttl.bz2')
        #exit(0)
        