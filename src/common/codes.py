from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef
import csv

class Codes(object):
    
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
                'fileName' : 'codes/belief.csv',
                'map' : dict()
            },
            self.namespaces['cedar']['city'] : {
                'fileName' : 'codes/city.csv',
                'map' : dict()
            },
            self.namespaces['cedar']['province'] : {
                'fileName' : 'codes/province.csv',
                'map' : dict()
            }
        }
        
        # Load the content of the files
        for dim in self.dimensions.values():
            print "Loading %s ..." % dim['fileName']            
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
            print "Got %d entries" % len(dim['map'].keys())
    
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
    