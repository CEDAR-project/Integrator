#!/usr/bin/python2
import json
import os
from jinja2 import Template
from util.sparql import SPARQLWrap
from rdflib.namespace import XSD
from modules.tablinker.namespace import TABLINKER

# TODO: How many rules per dataset ?
# TODO: Export all the queries outside of the main code 
# TODO: Move the queries to the visualisation frontend - copy provoviz.org to make the user wait
# TODO: Use http://www.chartjs.org/docs/#radar-chart

# Output of tablinker (sheets parsed / nb cells / nb titles / ...)

QUERIES = ['parsed_sheets', 'tablinker_output', 'nb_obs', 'dimension_usage']

import logging
log = logging.getLogger(__name__)

class StatsGenerator(object):
    def __init__(self, end_point, raw_graph_name, rules_graph_name,
                 release_graph_name, use_cache=False):
        '''
        Constructor
        '''
        self.end_point = end_point
        self.sparql_params = {'__RAW_DATA__': raw_graph_name,
                              '__RULES__'   : rules_graph_name,
                              '__RELEASE__' : release_graph_name}
        self.use_cache = use_cache
        
    def go(self, output_file_name):
        '''
        Compute all the statistics
        '''
        # Run all the queries
        results = {}
        if self.use_cache and os.path.isfile('/tmp/results.json'):
            log.info("Load cached data")
            with open('/tmp/results.json', 'r') as infile:
                results = json.load(infile)
        else:
            sparql = SPARQLWrap(self.end_point)
            for query_name in QUERIES:
                query_file = "{}/{}.sparql".format(os.path.dirname(__file__),
                                                   query_name)
                log.info("Execute %s" % query_file)
                query = open(query_file, 'r').read()
                r = sparql.run_select(query, self.sparql_params)
                parsed_results = self._parse_results(r)
                results[query_name] = parsed_results
                log.info("Results %s" % parsed_results)
            with open('/tmp/results.json', 'w') as outfile:
                json.dump(results, outfile)
        
            
        # Prepare the table with the overview for the sources
        table = {}
        for entry in results['parsed_sheets']:
            src = entry['src']
            table.setdefault(src, {})
            table[src]['sheets'] = "{}/{}".format(entry['nbsheetsparsed'],
                                                  entry['nbsheets'])
        for entry in results['tablinker_output']:
            src = entry['src']
            table.setdefault(src, {})
            header_type = entry['type'].replace(TABLINKER, 'tablinker:')
            table[src][header_type] = entry['total']
            
        # # Prepare the spider chart for the overview for the dimension used 
        spider_labels = []
        spider_data = []
        for entry in results['dimension_usage']:
            spider_data.append(int(entry['nbobs']))
            spider_labels.append(str(entry['dimension']))
            
        # Process the template
        data = {'table':table, 
                'spider':{ 'label': spider_labels, 'data':spider_data}}
        tmpl_file_name = "{}/stats.html".format(os.path.dirname(__file__))
        template = Template(open(tmpl_file_name, 'r').read())
        with open(output_file_name, 'w') as outfile:
            outfile.write(template.render(data))

    def _parse_results(self, results):
        output = []
        for entry in results:
            output_entry = {}
            for (k, v) in entry.iteritems():
                value = None
                if v['type'] == 'typed-literal':
                    if v['datatype'] == XSD.integer:
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
