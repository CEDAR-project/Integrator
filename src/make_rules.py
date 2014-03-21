#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef
import re
import uuid
import bz2
import operator
import os.path

from codes import Codes

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
      'sdmx-code':Namespace('http://purl.org/linked-data/sdmx/2009/code#'),
      'cedar':Namespace('http://cedar.example.org/ns#')
    }
    
    def __init__(self, codes, tablename, endpoint, namedgraph):
        """
        Constructor
        """
        # Keep parameters
        self.codes = codes
        self.tablename = tablename
        self.endpoint = endpoint
        self.namedgraph = namedgraph
        self.nbrules = 0
        
        # The graph that will be used to store the rules
        self.graph = ConjunctiveGraph()
        for namespace in self.namespaces:
            self.graph.namespace_manager.bind(namespace, self.namespaces[namespace])
        
    def go(self):
        """
        Start the job
        """
        print "Start processing %s" % self.tablename
        
        #####
        # Start with the column headers
        #####
        
        # Get a list of all the column headers
        headers = {}
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?header ?label ?parent from <GRAPH> where {
        ?header a <http://example.org/ns#ColumnHeader>.
        ?header <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
        ?header <http://example.org/ns#subColHeaderOf> ?parent.
        }
        """.replace('GRAPH',self.namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            resource = URIRef(result['header']['value'])
            headers[resource] = {}
            headers[resource]['label'] = result['label']['value']
            headers[resource]['parent'] = URIRef(result['parent']['value'])
        print "Process %d column headers" % len(headers)
        
        # Get a sublist of leaves
        leaves = []
        for header in headers.keys():
            ok = True
            for h in headers.keys():
                if headers[h]['parent'] == header:
                    ok = False
            if ok:
                leaves.append(header)
                
        # Process all the leaf headers, one by one
        for leaf in leaves:
            self.process_column_header(headers, leaf)
            
        #####
        # Move on to the row headers
        #####
        headers = {}
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?header ?label from <GRAPH> where {
        ?header a <http://example.org/ns#RowProperty>.
        ?header <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
        }
        """.replace('GRAPH',self.namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            resource = URIRef(result['header']['value'])
            headers[resource] = {}
            headers[resource]['label'] = result['label']['value']
        print "Process %d row properties" % len(headers)
        
        for (header, label) in headers.iteritems():
            self.process_row_header(header, label)
            
        #####
        # Done
        #####
        print "Done with %d rules" % self.nbrules
        
    def process_row_header(self, header, label):
        """
        Process a row header.
        We get a sample of the values found in the rows and guess the type
        from it
        """
        sample = []
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?label from <GRAPH> where {
        ?obs a <http://purl.org/linked-data/cube#Observation>.
        ?obs <DIM> ?label.
        } limit 20
        """.replace('GRAPH',self.namedgraph).replace('DIM', header)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            label = self._clean_string(result['label']['value'])
            if 'totaal' not in label:
                sample.append(label)
        
        # If the sample is empty forget about this header
        if len(sample) == 0:
            return
        
        # Tweak: try to see if we have a sample that correspond to years
        # see e.g. table VT_1859_02_H1
        years = True
        try:
            for entry in sample:
                y = int(entry)
                if y > 1971 or y < 1600:
                    years = False
        except ValueError:
            years = False
        # if the column contains years it should be a birth year
        if years:
            self.create_rule_set_dimension(header, self.namespaces['cedar']['birthYear'])
            return
        
        # Check if we can find the dimension associated to this header
        counts = {}
        for entry in sample:
            result = codes.detect_code(entry)
            if result != None:
                (dim,value) = result
                counts.setdefault(dim,0)
                counts[dim] = counts[dim] + 1
        
        # If we can't find any possible match, skip this header        
        if len(counts) == 0:
            return
        
        # Get the dimension with the highest count
        sorted_counts = sorted(counts.iteritems(), key=operator.itemgetter(1), reverse=True)
        (dimension,count) = sorted_counts[0]
                
        # Tweak: jobPosition can not be in a hierarchical position
        # verify that the header is not the sub header of something
        if dimension == self.namespaces['cedar']['occupationPosition']:
            sparql = SPARQLWrapper(self.endpoint)
            query = """
            ask from <GRAPH> {
            <DIM> <http://example.org/ns#subPropertyOf> ?x.
            } 
            """.replace('GRAPH',self.namedgraph).replace('DIM', header)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            if sparql.query().convert()['boolean']:
                return
        
        # Create a rule to bind the dimension to this header    
        self.create_rule_set_dimension(header, dimension)
        
    def process_column_header(self, headers, header):
        """
        Process a column header
        headers = set of all headers
        header = target header
        """
        # Try to find if it's a "totaal"
        header_with_total = self.contains_total(headers, header)
        if header_with_total == None:
            # The set of dimensions that will be filled in by detect_dimensions
            dimensions = set()
            
            # Try to detect dimensions in this header and those above it
            self.detect_dimensions(dimensions, headers, header)
            
            # Add all the results
            for dimension in dimensions:
                (source, dim) = dimension
                self.create_rule_add_dimension_value(header, source, dim)
        else:
            # Add a rule to ignore this observation
            self.create_rule_ignore_observation(header, header_with_total)
                
    
    def saveTo(self, filename):
        """
        Write the file to disk
        """
        file = bz2.BZ2File(filename, 'wb', compresslevel=9)
        turtle = self.graph.serialize(destination=None, format='turtle')
        file.writelines(turtle)
        file.close()
        
    def detect_dimensions(self, dimensions, headers, header):
        """
        Check for known labels
        """
        # Get the data
        data = headers[header]
        
        # Clean the label
        label_clean = self._clean_string(data['label'])
        
        # Check if we can find something
        result = codes.detect_code(label_clean)
        if result != None:
            dimensions.add((header, result))
        
        # Recurse
        parent = data['parent']
        if parent in headers:
            # Hot fix to skip vertical merge resulting in duplicate headers
            while parent == header:
                parent = headers[parent]['parent']
            if parent in headers:
                self.detect_dimensions(dimensions, headers, parent)
    
    def contains_total(self, headers, header):
        """
        Check if the header is about the total of something
        """
        # Get the data
        data = headers[header]
        
        # Clean the label
        label_clean = self._clean_string(data['label'])
        
        # Check if the label contains the string "totaal"
        if "totaal" in label_clean:
            return header
        
        # Recurse to upper level
        parent = data['parent']
        if parent in headers:
            # Hot fix to skip vertical merge resulting in duplicate headers
            while parent == header:
                parent = headers[parent]['parent']
            if parent in headers:
                return self.contains_total(headers, parent)
        
        return None
    
    def _clean_string(self, text):
        """
        Utility function to clean a string
        """
        # Remove some extra things
        text_clean = text.replace('.', '').replace('_', ' ').lower()
        # Shrink spaces
        text_clean = re.sub(r'\s+', ' ', text_clean)
        # Remove lead and trailing whitespaces
        text_clean = text_clean.strip()
        return text_clean
    
    def get_nb_rules(self):
        return self.nbrules
    
    def create_rule_add_dimension_value(self, target, source, dimensionvalue):
        """
        Create a new harmonization rule that assign a dimension and value
        to all the observations having the targetDimension as a dimension
        """
        (dimension, value) = dimensionvalue
        resource = URIRef(self.namespaces['rules'] + str(uuid.uuid1()))
        self.graph.add((resource,
                        RDF.type,
                        self.namespaces['harmonizer']['AddDimensionValue']))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['targetDimension'],
                        target))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['generatedFrom'],
                        source))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['dimension'],
                        dimension))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['value'],
                        value))
        self.nbrules = self.nbrules + 1
    
    def create_rule_set_dimension(self, target, dimension):
        """
        Create a new harmonization rule that assign a dimension to a given
        header. The actual value will be resolved at the creation of the
        harmonized cubes
        """
        resource = URIRef(self.namespaces['rules'] + str(uuid.uuid1()))
        self.graph.add((resource,
                        RDF.type,
                        self.namespaces['harmonizer']['SetDimension']))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['targetDimension'],
                        target))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['dimension'],
                        dimension))
        self.nbrules = self.nbrules + 1
        
    def create_rule_ignore_observation(self, target, source):
        """
        Create a new harmonization rule that tells to ignore observations
        associated to the target dimension
        """
        resource = URIRef(self.namespaces['rules'] + str(uuid.uuid1()))
        self.graph.add((resource,
                        RDF.type,
                        self.namespaces['harmonizer']['IgnoreObservation']))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['targetDimension'],
                        target))
        self.graph.add((resource,
                        self.namespaces['harmonizer']['generatedFrom'],
                        source))
        #self.nbrules = self.nbrules + 1 we don't really want only these
        
if __name__ == '__main__':
    # Parameters
    ngtemplate = 'http://lod.cedar-project.nl/resource/v2/TABLE'
    endpoint = 'http://lod.cedar-project.nl:8080/sparql/cedar'
    
    # Load the codes
    codes = Codes()
    
    # Get a list of tables (testing : 'BRT_1899_10_T_marked')
    tables = [table.strip() for table in open('tables.txt')]
    
    # Process each table one by one
    for table in tables:
        t = table.split('_')
        type = t[0]
        year = t[1]
        # Skip some tables for testing
        #if type != 'BRT' or year != '1920':
        #    continue
        filename = 'rules/' + table + '.ttl.bz2'
        if os.path.isfile(filename):
            print 'Skip {0} !'.format(filename)
            continue
        try:
            r = RuleMaker(codes, table, endpoint, ngtemplate.replace('TABLE', table))
            r.go()
            if r.get_nb_rules() != 0:
                r.saveTo(filename)
        except:
            print "Error processing the table !"
            pass
        #exit(0)
        