from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.term import URIRef, Literal

PAGE_SIZE = 10000

# Define the logger
import logging
log = logging.getLogger(__name__)

class SPARQLWrap(object):
    def __init__(self, end_point):
        '''
        Constructor
        '''
        # Set the end point
        self.end_point = end_point
        
    def run_select(self, query, params = None):
        '''
        Execute a SPARQL select
        '''
        sparql = SPARQLWrapper(self.end_point)
        sparql.setReturnFormat(JSON)
        
        if params != None:
            for (k,v) in params.iteritems():
                query = query.replace(k,v)
        sparql.setQuery(query)
        
        log.debug("Sending query to {} : {}".format(self.end_point, query))
        results = sparql.query().convert()
        
        return results["results"]["bindings"]
    
    def run_select_paginated(self, query, params = None):
        '''
        Execute a SPARQL select
        '''
        total_results = []
        offset = 0
        sparql = SPARQLWrapper(self.end_point)
        sparql.setReturnFormat(JSON)
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
            total_results = total_results + page_results["results"]["bindings"]
            nb_results = len(page_results["results"]["bindings"])
            next_page = (nb_results == PAGE_SIZE)
            offset = offset + PAGE_SIZE
        
        return total_results
    
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
    
        