#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import Namespace, URIRef, Variable
from rdflib.plugins.sparql import prepareQuery
import re
import rdflib
import bz2
import os.path

NG_TEMPLATE = 'http://lod.cedar-project.nl/resource/v2/TABLE'
END_POINT = 'http://lod.cedar-project.nl:8080/sparql/cedar'

class TableOverview(object):    
    def __init__(self, table):
        self.table = table
        self.rulesFile = 'rules/' + table + '.ttl.bz2'
        
    def go(self):
        if not os.path.isfile(self.rulesFile):
            return
        
        # Load all the relevant data
        col_headers = self.load_column_headers()
        row_headers = self.load_row_headers()
        rules = self.load_rules()
        
        # Look at the leaves of the column headers
        col_leaves = []
        for header in col_headers.keys():
            ok = True
            for h in col_headers.keys():
                if col_headers[h]['parent'] == header:
                    ok = False
            if ok:
                col_leaves.append(header)
                
        # Process all the leaf headers, one by one
        col_headers_entries = {}
        for leaf in sorted(col_leaves):
            label = self.get_leaf_label(col_headers, leaf, "")
            if leaf in rules:
                for rule in rules[leaf]:
                    if rule['type'] == 'IgnoreObservation':
                        description = 'Ignore observation'
                        col_headers_entries.setdefault(label, []).append(description)
                    elif rule['type'] == 'AddDimensionValue':
                        (dim, val) = rule['dimval']
                        val_txt = val.split('#')[1]
                        dim_txt = dim.split('#')[1]
                        description = 'Assign the code \"' + val_txt + '\" '
                        description += 'to the dimension \"' + dim_txt + '\"'
                        col_headers_entries.setdefault(label, []).append(description)
                    
                    
                    
        # Process the column headers
        row_headers_entries = {}
        for row in sorted(row_headers):
            if row in rules:
                for rule in rules[row]:
                    label = row_headers[row]['label']
                    if rule['type'] == 'SetDimension':
                        code_txt = rule['dimension'].split('#')[1]
                        description = 'Assign a value from the \"' + code_txt + '\" codes' 
                        row_headers_entries.setdefault(label, []).append(description)
        
        
        # Print out the output
        outputFileName = 'rules/' + self.table + '.md'
        outputFile = open(outputFileName, "wb")
        table_link = "https://github.com/cgueret/DataDump/blob/master/xls-marked/" + self.table + "_marked.xls?raw=true"
        outputFile.write("# Rules for the table [" + self.table + "](" + table_link + ")\n")
        
        outputFile.write("## Row properties\n")
        outputFile.write("| Title of the property | Rules |\n")
        outputFile.write("| --------------------- |:-----:|\n")
        for entry in row_headers_entries.iteritems():
            (label, txts) = entry
            txt = " *and* ".join(txts)
            outputFile.write("| %s | %s |\n" % (label, txt))
                
        outputFile.write("## Column properties\n")
        outputFile.write("| Title of the column | Rules |\n")
        outputFile.write("| --------------------- |:-----:|\n")
        for entry in col_headers_entries.iteritems():
            (label, txts) = entry
            txt = " *and* ".join(txts)
            outputFile.write("| %s | %s |\n" % (label, txt))
        outputFile.close()
                  
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
    
    def get_leaf_label(self, headers, header, label):
        # Get the data
        data = headers[header]
        
        # Clean the label
        label_clean = self._clean_string(data['label'])
        
        # Prepend it
        if label == "":
            label = label_clean
        else:
            label = label_clean + " >> " + label
            
        # Recurse
        parent = data['parent']
        if parent in headers:
            # Hot fix to skip vertical merge resulting in duplicate headers
            while parent == header:
                parent = headers[parent]['parent']
            if parent in headers:
                return self.get_leaf_label(headers, parent, label)
            else:
                return label
        else:
            return label
        
    def load_column_headers(self):
        namedgraph = NG_TEMPLATE.replace('TABLE', self.table)
        
        # Get a list of all the column headers
        headers = {}
        sparql = SPARQLWrapper(END_POINT)
        query = """
        select distinct ?header ?label ?parent from <GRAPH> where {
        ?header a <http://example.org/ns#ColumnHeader>.
        ?header <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
        ?header <http://example.org/ns#subColHeaderOf> ?parent.
        }
        """.replace('GRAPH', namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            resource = URIRef(result['header']['value'])
            headers[resource] = {}
            headers[resource]['label'] = result['label']['value']
            headers[resource]['parent'] = URIRef(result['parent']['value'])
        print "Found %d column headers" % len(headers)
        
        return headers
    
    def load_row_headers(self):
        namedgraph = NG_TEMPLATE.replace('TABLE', self.table)
        
        headers = {}
        sparql = SPARQLWrapper(END_POINT)
        query = """
        select distinct ?header ?label from <GRAPH> where {
        ?header a <http://example.org/ns#RowProperty>.
        ?header <http://www.w3.org/2004/02/skos/core#prefLabel> ?label.
        }
        """.replace('GRAPH', namedgraph)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            resource = URIRef(result['header']['value'])
            headers[resource] = {}
            headers[resource]['label'] = result['label']['value']
        print "Found %d row properties" % len(headers)
        
        return headers
    
    def load_rules(self):
        # Create the var to store the rules        
        rules = {}
        
        # Load the rules file
        g = rdflib.Graph()
        g.load(bz2.BZ2File(self.rulesFile), format="turtle")
        if len(g) == 0:
            return
        print "Loaded %d triple rules from %s" % (len(g), self.rulesFile)
        
        # Load the AddDimensionValue rules from the graph g
        q = prepareQuery("""
            select ?target ?dim ?value where {
                ?rule a harmonizer:AddDimensionValue.
                ?rule harmonizer:dimension ?dim.
                ?rule harmonizer:targetDimension ?target.
                ?rule harmonizer:value ?value.
            }
        """, initNs={ "harmonizer": self.namespaces['harmonizer'] })
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
        """, initNs={ "harmonizer": self.namespaces['harmonizer'] })
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
        """, initNs={ "harmonizer": self.namespaces['harmonizer'] })
        qres = g.query(q)
        for row in qres:
            target = row[Variable('target')]
            rule = {
                'type' : 'IgnoreObservation',
            }
            rules.setdefault(target, []).append(rule)
        
        return rules
    
if __name__ == '__main__':
    # table = 'BRT_1899_10_T'
    tables = [table.strip() for table in open('tables.txt')]
    
    for table in tables:
        table_overview = TableOverview(table)
        table_overview.go()

