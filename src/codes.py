from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef
import csv
import logging

class Codes(object):
    
    def __init__(self, configuration):
        self.conf = configuration
        self.log = logging.getLogger("RuleMaker")
        
        # Declare the dimensions
        self.dimensions = {
            self.conf.getURI('sdmx-dimension','sex') : {
                'fileName' : 'data/input/codes/sex.csv',
                'map' : dict()
            },
            self.conf.getURI('cedar','maritalStatus') : {
                'fileName' : 'data/input/codes/marital_status.csv',
                'map' : dict()
            },
            self.conf.getURI('cedar','occupationPosition') : {
                'fileName' : 'data/input/codes/occupation_position.csv',
                'map' : dict()
            },
            self.conf.getURI('cedar','occupation') : {
                'fileName' : 'data/input/codes/occupation.csv',
                'map' : dict()
            },
            self.conf.getURI('cedar','belief') : {
                'fileName' : 'data/input/codes/belief.csv',
                'map' : dict()
            },
            self.conf.getURI('cedar','city') : {
                'fileName' : 'data/input/codes/city.csv',
                'map' : dict()
            },
            self.conf.getURI('cedar','province') : {
                'fileName' : 'data/input/codes/province.csv',
                'map' : dict()
            }
        }
        
        # Load the content of the files
        for dim in self.dimensions.values():
            self.log.debug("Loading %s ..." % dim['fileName'])    
            f  = open(dim['fileName'], "rb")
            reader = csv.reader(f)
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
            f.close()
    
    def get_code(self, dimension, literal):
        """
        Return the code associated to a literal
        """
        if dimension in self.dimensions:
            if literal in self.dimensions[dimension]['map']:
                return self.dimensions[dimension]['map'][literal]
        return None

    def no_codes_for(self, dimension):
        """
        Return true if there is no code for a dimension
        """
        return dimension not in self.dimensions
    
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
    