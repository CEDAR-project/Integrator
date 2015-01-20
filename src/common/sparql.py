from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.term import URIRef, Literal

PAGE_SIZE = 10000

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
        total_results = []
        offset = 0
        sparql = SPARQLWrapper(self.conf.get_SPARQL())
        sparql.setReturnFormat(JSON)
        sparql.setCredentials('rdfread', 'red_fred')
        if params != None:
            for (k,v) in params.iteritems():
                query = query.replace(k,v)
        query = self.prefixes + query
        
        # Run the query page per page
        next_page = True
        while next_page:
            page_query = "%s LIMIT %d OFFSET %d" % (query, PAGE_SIZE, offset)
            sparql.setQuery(page_query)
            page_results = sparql.query().convert()
            total_results.append(page_results["results"]["bindings"])
            nb_results = len(page_results["results"]["bindings"])
            next_page = (nb_results == PAGE_SIZE)
            offset = offset + PAGE_SIZE
        
        return total_results
    
    def run_construct(self, query, params = None):
        '''
        Execute a SPARQL construct
        '''
        sparql = SPARQLWrapper(self.conf.get_SPARQL())
        if params != None:
            for (k,v) in params.iteritems():
                query = query.replace(k,v)
        query = self.prefixes + query
        sparql.setQuery(query)
        sparql.setCredentials('rdfread', 'red_fred')
        results = sparql.query().convert()
        return results
    
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
    
        