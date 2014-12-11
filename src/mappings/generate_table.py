#!/usr/bin/python2
from common.configuration import Configuration
from common.sparql import SPARQLWrap
import csv

# VT_1879_01_H1 Good example use-case for huizen
# VT_1930_02_T1 Tricky one for life tijd VT_1879_05_H4 too

# Seeds for age : Leeftijdsklasse, Geboortedatum, Geboortejaar, Ouderdom

class GenerateLists():
    '''
    This object is used to generate lists of header strings extracted from 
    the raw data
    '''
    def __init__(self, config):
        ''' Constructor '''
        self._conf = config
        self._sparql = SPARQLWrap(config)
        self._row_property_seeds = set()
        self.log = config.getLogger('GenerateLists')
        self._sparql_queries = []
        # self._sparql_queries.append(open('../Queries/find_column_headers.rq','r').read())
        self._sparql_row_property = open('../Queries/find_row_headers.rq', 'r').read()
        
    def add_seed_row_property(self, string):
        ''' Add a string to the list of strings to look for in row properties '''
        return self._row_property_seeds.add(string)
        
    def remove_seed_row_property(self, string):
        ''' Remove a string from the list of strings to look for in row properties '''
        return self._row_property_seeds.remove(string)
    
    def go(self):
        ''' Go over all the seeds and store the output '''
        output = []
        
        for seed in self._row_property_seeds:
            print 'Query for %s' % seed
            query = "%s" % self._sparql_row_property
            query = query.replace('__seed_text__', seed)
            results = self._sparql.run_select(query)
            distinct = set()
            print 'Number of results: %d' % len(results)
            for result in results:
                dataset = result['ds']['value']
                value = result['value']['value']
                if dataset + value not in distinct:
                    output.append([dataset, value.replace('\n', '').encode('utf8', 'replace')])
                    distinct.add(dataset + value)
            
        return output
    
 
if __name__ == '__main__':
    config = Configuration('config.ini')
    
    lists_generator = GenerateLists(config)
    lists_generator.add_seed_row_property('Werk')
    lists_generator.add_seed_row_property('Beroep')
    output = lists_generator.go()
    
    with open('/tmp/occupations.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for line in output:
            writer.writerow(line)
