import os
import pycurl

SPARQL = "http://lod.cedar-project.nl:8080/sparql"
SERVER = "http://lod.cedar-project.nl:8080/sparql-graph-crud"

def upload_graph(user, uri, turle_file):
    # Clear the previous graph
    c = pycurl.Curl()
    values = [("query", "CLEAR GRAPH <%s>" % uri)]
    c.setopt(c.URL, SPARQL)
    c.setopt(c.USERPWD, user)
    c.setopt(c.HTTPPOST, values)
    c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
    c.perform()
    c.close()

    # Upload the new data    
    c = pycurl.Curl()
    values = [("res-file", (pycurl.FORM_FILE, turle_file)),("graph-uri", graph)]
    c.setopt(c.URL, SERVER)
    c.setopt(c.USERPWD, user)
    c.setopt(c.HTTPPOST, values)
    c.setopt(c.HTTPHEADER, [ 'Content-Type:multipart/form-data', 'Expect: ']);
    c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
    c.perform()
    c.close()
    
if __name__ == '__main__':
    user = ':'.join([c.strip() for c in open('credentials-virtuoso.txt')])
    data = "/tmp/data.ttl"
    graph = "urn:graph:update:test:put"
    name = os.path.basename(data)
    upload_graph(user, graph, data)


# Push to OWLIM
# curl -X POST -H "Content-Type:application/x-turtle" -T config.ttl http://localhost:8080/openrdf-sesame/repositories/SYSTEM/rdf-graphs/service?graph=http://example.com#g1
# https://github.com/LATC/24-7-platform/blob/master/latc-platform/console/src/main/python/upload_files.py
# http://owlim.ontotext.com/display/OWLIMv52/OWLIM+FAQ

# Push to Virtuoso
# http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtGraphProtocolCURLExamples

