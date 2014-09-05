import pycurl
import bz2
import os
import sys
import traceback
import requests
import glob
import subprocess

SPARQL = "http://lod.cedar-project.nl:8080/sparql"
SERVER = "http://lod.cedar-project.nl:8080/sparql-graph-crud"
BUFFER = "data/tmp/buffer.nt"
MAX_NT = 240000

# Working POST
# curl --digest --user "dba:naps48*mimed" --verbose --url "http://lod.cedar-project.nl:8080/sparql-graph-crud?graph-uri=urn:graph:update:test:put" -X POST -T /tmp/data.ttl 
# http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtTipsAndTricksGuideDeleteLargeGraphs

class Pusher(object):
    def __init__(self):
        self.cred = ':'.join([c.strip() for c in open('credentials-virtuoso.txt')])
        self.user = self.cred.split(':')[0]
        self.pas = self.cred.split(':')[1]
        
    def clean_graph(self, uri):
        # Clear the previous graph
        query = """
        DEFINE sql:log-enable 3 
        CLEAR GRAPH <%s>
        """ % uri
        r = requests.post(SPARQL, auth=(self.user,self.pas), data={'query' : query})
        print r.status_code
    
    def upload_graph(self, uri, turle_file):
        try:
            data_file = turle_file
            if turle_file.endswith('.bz2'):
                data_file = '/tmp/data.ttl'
                f = open(data_file, 'wb')
                f.write(bz2.BZ2File(turle_file).read())
                f.close()
             
            r = requests.post(SERVER + "?graph-uri=" + uri,
                              (self.user,self.pas), data=file(data_file,'rb').read())
            print r.status_code
        except:
            traceback.print_exc(file=sys.stdout)
        
    def upload_directory(self, graph_uri, directory):
        # First remove the last BUFFER
        if os.path.isfile(BUFFER):
            os.unlink(BUFFER)
        
        # Concatenate all the source input to the BUFFER
        for input_file in glob.glob(directory):
            if input_file.endswith('.bz2'):
                f = open(BUFFER + '-part', 'wb')
                f.write(bz2.BZ2File(input_file).read())
                f.close()
                subprocess.call(["rapper", "-i guess -o ntriples " + BUFFER + '-part >> ' + BUFFER], stdout=sys.stdout) 
            else:
                subprocess.call("rapper -i guess -o ntriples " + input_file + ' >> ' + BUFFER, stderr=sys.stdout, shell=True) 
            
        # Load all the data
        input_file = open(BUFFER, 'rb')
        count = 0
        query = """
        DEFINE sql:log-enable 3 
        INSERT INTO <%s> {
        """ % graph_uri
        for triple in input_file.readlines():
            query = query + triple
            count = count + 1
            
            if count == MAX_NT:
                # Finish and send the query
                query = query + "}"
                r = requests.post(SPARQL, auth=(self.user,self.pas), data={'query' : query})
                print r.status_code
                
                # Restart
                count = 0
                query = """
                DEFINE sql:log-enable 3 
                INSERT INTO <%s> {
                """ % graph_uri

        # Send whatever is left over
        query = query + "}"
        r = requests.post(SPARQL, auth=(self.user,self.pas), data={'query' : query})
        print r.status_code
            
if __name__ == '__main__':
    graph = "urn:graph:update:test:put"
    pusher = Pusher()
    pusher.clean_graph(graph)
    pusher.upload_directory(graph, "data-test/rdf/*")

# Push to OWLIM
# curl -X POST -H "Content-Type:application/x-turtle" -T config.ttl http://localhost:8080/openrdf-sesame/repositories/SYSTEM/rdf-graphs/service?graph=http://example.com#g1
# https://github.com/LATC/24-7-platform/blob/master/latc-platform/console/src/main/python/upload_files.py
# http://owlim.ontotext.com/display/OWLIMv52/OWLIM+FAQ

# Push to Virtuoso
# http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtGraphProtocolCURLExamples

