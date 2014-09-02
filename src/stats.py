#!/usr/bin/python2
import json
from common.sparql import SPARQLWrap
from common.configuration import Configuration, RAW_XLS_PATH
from jinja2 import Template
import glob
import os
from xlrd import open_workbook
import logging

log = logging.getLogger("Stats")

class StatsGenerator(object):
    
    def __init__(self, config):
        self._conf = config
        self._sparql = SPARQLWrap(config)
        self._params = {'RAW':'<urn:graph:cedar:raw-rdf>',
                         'RULES':'<urn:graph:cedar:harmonization_rules>',
                         'RELEASE' : '<urn:graph:cedar:harmonised_data>'}

    def get_numbers(self):
        output = {} 
        self._get_numbers_raw_rdf(output)
        self._get_numbers_harmonized_data(output)
        return output
    
    def _get_numbers_harmonized_data(self, output):
        '''
        Get numbers about the harmonization process
        '''
        # Get the number of observations that are ignored
        log.info("Count ignored observations")
        query = """
        SELECT (count(distinct ?obs) as ?total) FROM RAW FROM RULES WHERE {
        ?obs a qb:Observation.
        ?obs ?p ?o .
        ?rule a harmonizer:IgnoreObservation.
        ?rule harmonizer:targetDimension ?p.
        ?rule harmonizer:targetValue ?o.
        } """
        results = self._sparql.run_select(query, self._params)
        output['nb_observations_ignored'] = int(results[0]['total']['value'])

        # Get the number of harmonized observations
        log.info("Count total observations in release")
        query = """
        SELECT (count(distinct ?obs) as ?total) FROM RELEASE WHERE {
        ?obs a qb:Observation.
        } """
        results = self._sparql.run_select(query, self._params)
        output['nb_observations_released'] = int(results[0]['total']['value'])
        
        # What do we find ?
        log.info("Count occurences of all the dimensions")
        output['nb_occurences'] = {}
        target_dims = {'province' : 'cedar:province',
                       'city' : 'cedar:city',
                       'belief' : 'cedar:belief',
                       'occupation' : 'cedar:occupation',
                       'occupation position' : 'cedar:occupationPosition',
                       'marital status' : 'cedar:maritalStatus',
                       'sex' : 'sdmx-dimension:sex'}
        for (name,dim) in target_dims.iteritems():
            query = """
            SELECT (count(distinct ?obs) as ?total) FROM RAW FROM RULES WHERE {
            ?obs a qb:Observation.
            ?obs ?p ?o .
            ?rule a harmonizer:SetValue.
            ?rule harmonizer:targetDimension ?p.
            ?rule harmonizer:targetValue ?o.
            ?rule harmonizer:dimension DIM.
            } """.replace('DIM', dim)
            results = self._sparql.run_select(query, self._params)
            count = int(results[0]['total']['value'])
            output['nb_occurences'][name] = count
        print output['nb_occurences']
        
    def _get_numbers_raw_rdf(self, output):
        '''
        Get all the figures related to the raw data set
        '''
        output['nbs_per_src'] = {}
        
        # Get the number of datasets per source
        log.info("Count datasets per source")
        output['nb_datasets'] = 0
        query = """
        SELECT distinct ?location (Count(distinct ?d) AS ?total) FROM RAW WHERE {
        ?d a qb:DataSet.
        ?d prov:wasDerivedFrom ?src.
        ?src a dcat:DataSet.
        ?src dcat:distribution ?file.
        ?file dcterms:accessURL ?location.
        } group by ?location
        """
        results = self._sparql.run_select(query, self._params)
        for result in results:
            src = result['location']['value'].split('/')[-1].split('.')[0]
            count = int(result['total']['value'])
            output['nbs_per_src'].setdefault(src, {})
            output['nbs_per_src'][src]['datasets'] = count
            output['nb_datasets'] = output['nb_datasets'] + count
        
        # Get the number of expected datasets
        log.info("Count the number of expected datasets")
        output['nb_datasets_expected'] = 0
        raw_xls_files = glob.glob(config.getPath(RAW_XLS_PATH))
        for raw_xls_file in sorted(raw_xls_files):
            name = os.path.basename(raw_xls_file).split('.')[0]
            wb = open_workbook(raw_xls_file, formatting_info=False, on_demand=True)
            output['nbs_per_src'].setdefault(name, {})
            output['nbs_per_src'][name]['datasets_expected'] = wb.nsheets
            output['nb_datasets_expected'] = output['nb_datasets_expected'] + wb.nsheets 
            
        # Get the number of observations per dataset
        log.info("Count the number of observations per source")
        output['nb_observations'] = 0
        query = """
        SELECT distinct ?location (Count(distinct ?obs) AS ?total) FROM RAW WHERE {
        ?obs a qb:Observation.
        ?obs qb:dataSet ?d.
        ?d a qb:DataSet.
        ?d prov:wasDerivedFrom ?src.
        ?src a dcat:DataSet.
        ?src dcat:distribution ?file.
        ?file dcterms:accessURL ?location.
        } group by ?location
        """
        results = self._sparql.run_select(query, self._params)
        for result in results:
            src = result['location']['value'].split('/')[-1].split('.')[0]
            count = int(result['total']['value'])
            output['nbs_per_src'].setdefault(src, {})
            output['nbs_per_src'][src]['observations'] = count
            output['nb_observations'] = output['nb_observations'] + count
    
if __name__ == '__main__':
    config = Configuration('config.ini')
    stats = StatsGenerator(config)
    
    # Get the numbers or reload them from a stored dump
    data = None
    #with open('/tmp/stats.json', 'w') as outfile:
    #    data = stats.get_numbers()
    #    json.dump(data, outfile)
    if data == None:
        with open('/tmp/stats.json', 'r') as infile:
            data = json.load(infile)
    
    # Compute the overview
    data['nbs_per_src_overview'] = {}
    for (k, v) in data['nbs_per_src'].iteritems():
        key = '_'.join(k.split('_')[:2]) 
        data['nbs_per_src_overview'].setdefault(key, {'observations':0, 'datasets':0, 'datasets_expected':0})
        if 'datasets' in v:
            data['nbs_per_src_overview'][key]['datasets'] += v['datasets']
        if 'datasets_expected' in v:
            data['nbs_per_src_overview'][key]['datasets_expected'] += v['datasets_expected']
        if 'observations' in v:
            data['nbs_per_src_overview'][key]['observations'] += v['observations']
    print data['nbs_per_src_overview']
    
    # Process the template
    template = Template(open('src/stats.html', 'r').read())
    output = template.render(data)
    with open('/tmp/stats.html', 'w') as outfile:
        outfile.write(output)

    
