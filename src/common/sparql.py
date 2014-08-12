from SPARQLWrapper import SPARQLWrapper, JSON
from SPARQLWrapper.Wrapper import RDF, N3
from rdflib.term import URIRef, Literal

class SPARQLWrap(object):
    def __init__(self, configuration):
        '''
        Constructor
        '''
        # Keep parameters
        self.conf = configuration

        # Store prefixes
        self.prefixes = self.conf.get_prefixes()
        
    def run_select(self, query, params = None):
        '''
        Execute a SPARQL select
        '''
        sparql = SPARQLWrapper(self.conf.get_SPARQL())
        if params != None:
            for (k,v) in params.iteritems():
                query = query.replace(k,v)
        sparql.setQuery(self.prefixes + query)
        sparql.setReturnFormat(JSON)
        sparql.setCredentials('rdfread', 'red_fred')
        results = sparql.query().convert()
        return results["results"]["bindings"]

    def format(self, entry):
        v = None
        if entry['type'] == 'uri':
            v = URIRef(entry['value'])
        else:
            if 'datatype' in entry:
                v = Literal(entry['value'], datatype=entry['datatype'])
            else:
                v = Literal(entry['value'])
        return v
    
        