import bz2
import os
import requests
import multiprocessing
from subprocess import check_call

BUFFER = "/tmp/buffer.txt"
MAX_NT = 1000  # hard max apparently for Virtuoso

# Define the logger
import logging
log = logging.getLogger(__name__)

# Working POST
# curl --digest --user "dba:naps48*mimed" --verbose --url "http://lod.cedar-project.nl:8080/sparql-graph-crud?graph-uri=urn:graph:update:test:put" -X POST -T /tmp/data.ttl 
# http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtTipsAndTricksGuideDeleteLargeGraphs

logging.getLogger("requests").setLevel(logging.WARNING)

def _push_chunk_thread(parameters):
    graph_uri = parameters['graph_uri']
    chunk = parameters['chunk']
    sparql = parameters['sparql']
    user = parameters['user']
    secret = parameters['secret']
    query = """
    DEFINE sql:log-enable 3 
    INSERT INTO %s {
    """ % graph_uri
    query = query + chunk + "}"
    r = requests.post(sparql, auth=(user, secret), data={'query' : query})
    if r.status_code != 200:
        log.error("{} : {}".format(r.status_code, r.text.replace('\n', '')))
    
class Pusher(object):
    def __init__(self, sparql, user, secret):
        self.user = user
        self.secret = secret
        self.sparql = sparql
        
    def clean_graph(self, uri):
        # Clear the previous graph
        query = """
        DEFINE sql:log-enable 3 
        CLEAR GRAPH %s
        """ % uri
        r = requests.post(self.sparql, 
                          auth=(self.user, self.secret),
                          data={'query' : query})
        if r.status_code != 200:
            log.error("{} : {}".format(r.status_code, r.text.replace('\n', '')))
    
    def upload_file(self, graph_uri, input_file):
        # First remove the last BUFFER        
        if os.path.isfile(BUFFER):
            os.unlink(BUFFER)
        
        # Serialise the triples as ntriples in BUFFER
        DEVNULL = open(os.devnull, 'wb')
        try:
            if input_file.endswith('.bz2'):
                f = open(BUFFER + '-unzip', 'wb')
                f.write(bz2.BZ2File(input_file).read())
                f.close()
                check_call("rapper -i turtle -o ntriples " + BUFFER + '-unzip > ' + BUFFER, stderr=DEVNULL, shell=True)
            else:
                check_call("rapper -i turtle -o ntriples " + input_file + ' > ' + BUFFER, stderr=DEVNULL, shell=True) 
        finally:
            DEVNULL.close()              
        
        # Load all the data into chunks
        tasks = []
        chunk = ""
        input_file = open(BUFFER, 'rb')            
        count = 0
        for triple in input_file.readlines():
            chunk = chunk + triple
            count = count + 1
            # If we reach the max, store the chunk
            if count == MAX_NT:
                tasks.append({"chunk": chunk,
                              "graph_uri":graph_uri,
                              "sparql": self.sparql,
                              "user": self.user,
                              "secret": self.secret})
                count = 0
                chunk = ""
        # Store the last chunk
        tasks.append({"chunk": chunk, 
                      "graph_uri":graph_uri,
                      "sparql": self.sparql,
                      "user": self.user,
                      "secret": self.secret})
        input_file.close()
                    
        # Send everything !
        pool_size = 8  # Try to still not hammer Virtuoso too much
        pool = multiprocessing.Pool(processes=pool_size)
        pool.map(_push_chunk_thread, tasks)
        pool.close()
        pool.join()
                        
        
if __name__ == '__main__':
    graph = "urn:graph:update:test:put"
    pusher = Pusher("http://lod.cedar-project.nl:8080/sparql")
    pusher.clean_graph(graph)
    pusher.upload_directory(graph, "data-test/rdf/*")

# Push to OWLIM
# curl -X POST -H "Content-Type:application/x-turtle" -T config.ttl http://localhost:8080/openrdf-sesame/repositories/SYSTEM/rdf-graphs/service?graph=http://example.com#g1
# https://github.com/LATC/24-7-platform/blob/master/latc-platform/console/src/main/python/upload_files.py
# http://owlim.ontotext.com/display/OWLIMv52/OWLIM+FAQ

# Push to Virtuoso
# http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtGraphProtocolCURLExamples

