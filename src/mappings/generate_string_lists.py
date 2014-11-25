#!/usr/bin/python2
from common.configuration import Configuration
from common.sparql import SPARQLWrap

class GenerateLists():
    '''
    This object is used to generate lists of string extracted from 
    the raw data
    '''
    def __init__(self, config):
        ''' Constructor '''
        self._conf = config
        self._sparql = SPARQLWrap(config)
        self._seeds= set()
        self.log = config.getLogger('GenerateLists')
        self._sparql_query = open('../Queries/find_column_headers.rq','r').read()
        self._output = []
        
    def add_seed(self, string):
        ''' Add a string to the list of seed strings '''
        return self._seeds.add(string)
        
    def remove_seed(self, string):
        ''' Remove a string from the list of seed strings '''
        return self._seeds.remove(string)
    
    def go(self):
        ''' Go over all the seeds and store the output '''
        for seed in self._seeds:
            query = "%s" % self._sparql_query
            query = query.replace('__seed_text__', seed)
            results = self._sparql.run_select(query)
            for result in results:
                sheet = result['sheet']['value']
                cell = result['cell']['value']
                value = result['value']['value']
                sheet = sheet.replace('http://lod.cedar-project.nl:8888/cedar/resource/', 'cedar:')
                cell = cell.replace('http://lod.cedar-project.nl:8888/cedar/resource/', 'cedar:')
                line = '"%s","%s","%s"' % (sheet,cell,value)
                self._output.append(line)
                
    def save(self, output_file_name):
        output = open(output_file_name, 'w')
        for line in self._output:
            output.write(line.encode('utf8', 'replace'))
            output.write('\n')
        output.close()
        
if __name__ == '__main__':
    config = Configuration('config.ini')
    lists_generator = GenerateLists(config)
    lists_generator.add_seed("Roomsch katholiek armengesticht")
    lists_generator.go()
    lists_generator.save('housing_types.csv')
    