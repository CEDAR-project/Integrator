#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef, XSD
from  rdflib.plugins.sparql import prepareQuery
import re
import uuid
import rdflib
import bz2
import os.path

from codes import Codes

class CubeMaker(object):
    endpoint = 'http://lod.cedar-project.nl:8080/sparql/cedar'
    namespaces = {
      'dcterms':Namespace('http://purl.org/dc/terms/'), 
      'skos':Namespace('http://www.w3.org/2004/02/skos/core#'), 
      'tablink':Namespace('http://example.org/ns#'), 
      'harmonizer':Namespace('http://harmonizer.example.org/ns#'),
      'rules':Namespace('http://rules.example.org/resource/'),
      'qb':Namespace('http://purl.org/linked-data/cube#'), 
      'owl':Namespace('http://www.w3.org/2002/07/owl#'),
      'sdmx-dimension':Namespace('http://purl.org/linked-data/sdmx/2009/dimension#'),
      'sdmx-code':Namespace('http://purl.org/linked-data/sdmx/2009/code#'),
      'cedar':Namespace('http://cedar.example.org/ns#'),
      'cedardata':Namespace('http://cedar.example.org/resource/'),
    }
    
    def __init__(self, cube_name, data):
        """
        Constructor
        """
        # Keep parameters
        self.cube_name = cube_name
        self.data = data
        self.nb_obs = 0
        
        # The graph that will be used to store the rules
        self.graph = ConjunctiveGraph()
        for namespace in self.namespaces:
            self.graph.namespace_manager.bind(namespace, self.namespaces[namespace])
        
        # Initialise the bits about the data set
        self.dataset_resource = URIRef(self.namespaces['cedardata'] + self.cube_name)
        self.graph.add((self.dataset_resource, 
                        RDF.type, 
                        self.namespaces['qb']['DataSet']))
        self.graph.add((self.dataset_resource,
                        self.namespaces['cedar']['censusYear'],
                        Literal(self.data['year'], datatype=XSD.integer)))
        self.graph.add((self.dataset_resource,
                        self.namespaces['cedar']['censusType'],
                        Literal(self.data['census_type'])))
        
    def go(self):
        """
        Start the job
        """
        # Iterate over all the sources
        for source in self.data['sources']:
            # See if we have harmonization rules for this file
            rulesFile = 'rules/' + source + '.ttl.bz2'
            if not os.path.isfile(rulesFile):
                print 'No harmonization rules for {0} !'.format(source)
                continue
            self.processSource(source, rulesFile)

    def processSource(self, source, rulesFile):
        """
        Process a single source of data
        """
        namedgraph = 'http://lod.cedar-project.nl/resource/v2/TABLE'.replace('TABLE', source)

        # Create the var to store the rules        
        rules = {}
        
        # Load the rules file
        g = rdflib.Graph()
        g.load(bz2.BZ2File(rulesFile), format="turtle")
        if len(g) == 0:
            return
        print "Loaded %d triple rules from %s" % (len(g), rulesFile)
        
        # Load the AddDimensionValue rules from the graph g
        q = prepareQuery("""
            select ?target ?dim ?value where {
                ?rule a harmonizer:AddDimensionValue.
                ?rule harmonizer:dimension ?dim.
                ?rule harmonizer:targetDimension ?target.
                ?rule harmonizer:value ?value.
            }
        """, initNs = { "harmonizer": self.namespaces['harmonizer'] })
        qres = g.query(q)
        for row in qres:
            (target, dim, value) = row
            rule = {
                'type' : 'AddDimensionValue',
                'dimval' : (dim, value)
            }
            rules.setdefault(target, []).append(rule)

        # Load the SetDimension rules from the graph g
        q = prepareQuery("""
            select ?target ?dim where {
                ?rule a harmonizer:SetDimension.
                ?rule harmonizer:dimension ?dim.
                ?rule harmonizer:targetDimension ?target.
            }
        """, initNs = { "harmonizer": self.namespaces['harmonizer'] })
        qres = g.query(q)
        for row in qres:
            (target, dim) = row
            rule = {
                'type' : 'SetDimension',
                'dimension' : dim
            }
            rules.setdefault(target, []).append(rule)
        
        # Load the IgnoreObservation rules from the graph g
        q = prepareQuery("""
            select ?target where {
                ?rule a harmonizer:IgnoreObservation.
                ?rule harmonizer:targetDimension ?target.
            }
        """, initNs = { "harmonizer": self.namespaces['harmonizer'] })
        qres = g.query(q)
        for row in qres:
            (target) = row
            rule = {
                'type' : 'IgnoreObservation',
            }
            rules.setdefault(target, []).append(rule)
            
        # Get all the observations in several pages
        print "Query for observations ..."
        observations = {}
        sparql = SPARQLWrapper(self.endpoint)
        query = """
        select distinct ?obs ?p ?o from <GRAPH> where {
        ?obs a <http://purl.org/linked-data/cube#Observation>.
        ?obs ?p ?o.
        } 
        """.replace('GRAPH',namedgraph)
        query = query + " limit 100"
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            obs_r = URIRef(result['obs']['value'])
            v = None
            if result['o']['type'] == 'uri':
                v = URIRef(result['o']['value'])
            else:
                if 'datatype' in result['o']:
                    v = Literal(result['o']['value'], datatype=result['o']['datatype'])
                else:
                    v = Literal(result['o']['value'])
            observations.setdefault(obs_r,[]).append((URIRef(result['p']['value']),v))
        print "Will process %d observations" % len(observations)
        
        # Process observations
        for (obs,desc) in observations.iteritems():  
            self.processObservation(rules, source, obs, desc)
    
        # Update on the console
        print "The number of harmonized observations is now %d" % self.nb_obs
        
    def processObservation(self, rules, table, observation, description):
        """
        Process a specific source observation and add to the graph the
        harmonized version of it, using the rules from self.rules
        """
        # Check that the observation is an int, skip it otherwise (a bit hacky now)
        #http://example.org/resource/BRT_1920_01_S1_marked/populationSize
        pop_size = URIRef("http://example.org/resource/"+table+"_marked/populationSize")
        has_pop_size = False
        pop_size_val = Literal("")
        for (p, o) in description:
            if p == pop_size:
                has_pop_size = True
                pop_size_val = o
        if not has_pop_size or type(pop_size_val.toPython()) is not long:
            return
        
        # Get all the mapped dimensions
        harmonized_po = []
        for (p,o) in description:
            if o in rules:
                # These are rules for column headers
                rules_set = rules[o]
                for r in rules_set:
                    if r['type'] == 'IgnoreObservation':
                        # We need to ignore the observation, it's a sum of something
                        return
                    elif r['type'] == 'AddDimensionValue':
                        # Got a p,o pair to bind to the thing
                        harmonized_po.append(r['dimval'])
                    elif r['type'] == 'SetDimension':
                        # should not happen, maybe raise an exception
                        pass
                    else:
                        # Unknown rule ?!
                        pass
            elif p in rules:
                # These are rules that applies to row properties
                rules_set = rules[p]
                for r in rules_set:
                    if r['type'] == 'SetDimension':
                        raw_value = o
                        dim = r['dimension']
                        if codes.no_codes_for(dim):
                            # If there is no code just put the raw value
                            harmonized_po.append((dim, raw_value))
                        elif type(raw_value.toPython()) == unicode:
                            # try to map the value to a code
                            cleaned_raw_value = self._clean_string(raw_value.toPython())
                            c = codes.get_code(dim, cleaned_raw_value)
                            if c != None:
                                harmonized_po.append((dim, c))
                        else:
                            # Just ignore this dimension
                            pass
                    elif r['type'] == 'IgnoreObservation' or r['type'] == 'AddDimensionValue':
                        # should not happen, maybe raise an exception
                        pass
                    else:
                        # Unknown rule ?!
                        pass
            else:
                # No rule that apply to either the p or the o
                # no worries, that can happen
                pass
        
        if len(harmonized_po) > 0:
            # Add the new observation to the graph
            resource = URIRef(self.namespaces['cedardata'] + self.cube_name + "/" + str(self.nb_obs))
            self.nb_obs = self.nb_obs + 1
            self.graph.add((resource,
                            RDF.type,
                            self.namespaces['qb']['Observation']))
            self.graph.add((resource,
                            self.namespaces['cedar']['sourceObservation'],
                            observation))
            self.graph.add((resource,
                            self.namespaces['cedar']['populationSize'],
                            pop_size_val))
            self.graph.add((resource,
                            self.namespaces['qb']['dataSet'],
                            self.dataset_resource))
            for (p,o) in harmonized_po:
                self.graph.add((resource, p, o))
            
    def saveTo(self, filename):
        """
        Write the file to disk
        """
        file = bz2.BZ2File(filename, 'wb', compresslevel=9)
        turtle = self.graph.serialize(destination=None, format='turtle')
        file.writelines(turtle)
        file.close()

    def get_nb_obs(self):
        """
        Return the number of observations in the harmonized cube
        """
        return self.nb_obs
 
    def _clean_string(self, text):
        """
        Utility function to clean a string
        """
        # Remove some extra things
        text_clean = text.replace('.', '').replace('_', ' ').lower()
        # Shrink spaces
        text_clean = re.sub(r'\s+', ' ', text_clean)
        # Remove lead and trailing whitespaces
        text_clean = text_clean.strip()
        return text_clean
    
if __name__ == '__main__':
    # table = 'BRT_1899_10_T_marked'
    # Load the list of tables
    tables = [table.strip() for table in open('tables.txt')]
    
    # Load the codes
    codes = Codes()
    
    # We are going to make one cube per type/year combination, map the sources
    cubes = {}
    for table in tables:
        t = table.split('_')
        census_type = t[0]
        year = t[1]
        cube_name = "{}-{}".format(census_type, year)
        cubes.setdefault(cube_name, {})
        cubes[cube_name].setdefault('sources',[]).append(table)
        cubes[cube_name]['census_type'] = census_type
        cubes[cube_name]['year'] = year
        
    print "About to write {} cubes".format(len(cubes))
    
    # Go for it, one by one
    for cube_name in sorted(cubes.keys()):
        # Kind of debug code to focus on one cube
        #if cubes[cube_name]['census_type'] != 'VT' or cubes[cube_name]['year'] != '1830':
        #    continue
        print "Processing " + cube_name
        cube_maker = CubeMaker(cube_name, cubes[cube_name])
        cube_maker.go()
        if cube_maker.get_nb_obs() != 0:
            cube_maker.saveTo('cubes/' + cube_name + '.ttl.bz2')
    