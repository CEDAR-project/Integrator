from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef
import csv

class Codes(object):
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
    
    def __init__(self):
        # Declare the dimensions
        self.dimensions = {
            self.namespaces['sdmx-dimension']['sex'] : {
                'fileName' : 'codes/sex.csv',
                'map' : dict()
            },
            self.namespaces['cedar']['maritalStatus'] : {
                'fileName' : 'codes/marital_status.csv',
                'map' : dict()
            },
            self.namespaces['cedar']['occupationPosition'] : {
                'fileName' : 'codes/occupation_position.csv',
                'map' : dict()
            },
            self.namespaces['cedar']['occupation'] : {
                'fileName' : 'codes/occupation.csv',
                'map' : dict()
            },
            self.namespaces['cedar']['belief'] : {
                'fileName' : 'codes/religion.csv',
                'map' : dict()
            }
        }
        
        # Load the content of the files
        for dim in self.dimensions.values():
            print "Loading %s ..." % dim['fileName']            
            file  = open(dim['fileName'], "rb")
            reader = csv.reader(file)
            header_row = True
            for row in reader:
                # Skip the header
                if header_row:
                    header_row = False
                    continue
                # Skip empty lines
                if len(row) != 2:
                    continue
                if row[1].startswith("http"):
                    dim['map'][row[0]] = URIRef(row[1])
                else:
                    dim['map'][row[0]] = Literal(row[1])
            file.close()
            print "Got %d entries" % len(dim['map'].keys())
    
    def get_code(self, dimension, literal):
        """
        Return the code associated to a literal
        """
        if dimension in self.dimensions:
            if literal in self.dimensions[dimension]['map']:
                return self.dimensions[dimension]['map'][literal]
        return None

    def detect_code(self, literal):
        """
        Return the first match (dim, value) for the literal
        """
        for (dimension,data) in self.dimensions.iteritems():
            if literal in data['map']:
                return (dimension, data['map'][literal])
        return None
    
if __name__ == '__main__':
    codes = Codes()
    