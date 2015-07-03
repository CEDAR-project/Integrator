#!/usr/bin/python2
import json
import glob
import os
import logging
from common.sparql import SPARQLWrap
from common.configuration import Configuration
from jinja2 import Template

# TODO: How many rules per dataset ?
# TODO: Export all the queries outside of the main code 
# TODO: Move the queries to the visualisation frontend - copy provoviz.org to make the user wait

OVERVIEW = 'nb_parsed_sheets_overview'

QUERIES = {
           #'nb_triples' : 'count.rq',
           'nb_parsed_sheets' : 'parsed_sheets.rq',
           'get_dimensions' : 'get-dimensions.rq'
           }


def parse_results(results):
    output = []
    for entry in results:
        output_entry = {}
        for (k, v) in entry.iteritems():
            value = None
            if v['type'] == 'typed-literal':
                if v['datatype'] == 'http://www.w3.org/2001/XMLSchema#integer':
                    value = int(v['value'])
                else:
                    value = v['value']
            elif v['type'] == 'literal':
                value = str(v['value'])
            elif v['type'] == 'uri':
                value = v['value']
            else:
                value = v['value']
            output_entry[k] = value
        output.append(output_entry)
    return output

class StatsGenerator(object):
    def __init__(self, config, queries_path):
        '''
        Constructor
        '''
        self._conf = config
        self._queries_path = queries_path
        self._log = config.getLogger('Stats')
        self._sparql = SPARQLWrap(config)
        self._params = {'RAW'     : config.get_graph_name('raw-data'),
                        'RULES'   : config.get_graph_name('rules'),
                        'RELEASE' : config.get_graph_name('release')}
        self._stats = {}
        
    def go(self):
        '''
        Compute all the statistics
        '''
        # Run all the queries
        for (query_name, query_file) in QUERIES.iteritems():
            self._log.info("Execute %s" % query_name)
            query_text = open(self._queries_path + "/" + query_file).read()
            results = self._sparql.run_select(query_text, self._params)
            parsed_results = parse_results(results)
            self._stats[query_name] = parsed_results
    
        # Do a bit of post-processing to compute aggregates for TYPE_YEAR
        overview = {}
        for entry in self._stats['nb_parsed_sheets']:
            key = '_'.join(entry['datasetname'].split('_')[:2])
            overview.setdefault(key, {'expected':0, 'total':0})
            if 'nbsheets' in entry:
                overview[key]['expected'] += int(entry['nbsheets'])
            if 'total' in entry:
                overview[key]['total'] += int(entry['total'])
        self._stats[OVERVIEW] = []
        for key in sorted(overview.keys()):
            entry = {'datasetname' : key,
                     'nbsheets' : overview[key]['expected'], 
                     'total' : overview[key]['total']}
            self._stats[OVERVIEW].append(entry)
        
    def save_to(self, json_file_name):
        '''
        Save all the stats as a JSON document
        '''
        with open(json_file_name, 'w') as outfile:
            json.dump(self._stats, outfile)
             
if __name__ == '__main__':
    # Load the configuration file
    config = Configuration('config.ini')
    
    # Initialise the stats generator
    stats_generator = StatsGenerator(config, 'Queries/')
    
    # Go !
    stats_generator.go()
    
    # Save the output
    stats_generator.save_to('/tmp/stats.json')
    
    # Process the template
    #template = Template(open('src/stats.html', 'r').read())
    #output = template.render(data)
    #with open('/tmp/stats.html', 'w') as outfile:
    #    outfile.write(output)

    
