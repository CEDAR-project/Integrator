import pycurl
import bz2
import os
import sys
import traceback
import requests

SPARQL = "http://lod.cedar-project.nl:8080/sparql"
SERVER = "http://lod.cedar-project.nl:8080/sparql-graph-crud"

# Working POST
# curl --digest --user "dba:naps48*mimed" --verbose --url "http://lod.cedar-project.nl:8080/sparql-graph-crud?graph-uri=urn:graph:update:test:put" -X POST -T /tmp/data.ttl 

class Pusher(object):
    def __init__(self):
        self.user = ':'.join([c.strip() for c in open('credentials-virtuoso.txt')])
        
    def clean_graph(self, uri):
        # Clear the previous graph
        c = pycurl.Curl()
        values = [("query", "CLEAR GRAPH <%s>" % uri)]
        c.setopt(c.URL, SPARQL)
        c.setopt(c.USERPWD, self.user)
        c.setopt(c.HTTPPOST, values)
        c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
        c.perform()
        c.close()
    
    def upload_graph_small(self, uri, turle_file):
        try:
            data_file = turle_file
            if turle_file.endswith('.bz2'):
                data_file = 'tmp/data.ttl'
                f = open(data_file, 'wb')
                f.write(bz2.BZ2File(turle_file).read())
                f.close()
                
            # Upload the new data    
            c = pycurl.Curl()
            values = [("res-file", (pycurl.FORM_FILE, data_file)),("graph-uri", uri)]
            c.setopt(c.URL, SERVER)
            c.setopt(c.USERPWD, self.user)
            c.setopt(c.HTTPPOST, values)
            header = [ 'Content-Type:multipart/form-data', 'Expect: ']
            c.setopt(c.HTTPHEADER, header);
            #c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
            c.setopt(c.VERBOSE, True)
            c.perform()
            print c.getinfo(pycurl.HTTP_CODE), c.getinfo(pycurl.EFFECTIVE_URL)
            c.close()
        except:
            traceback.print_exc(file=sys.stdout)

    def upload_graph(self, uri, turle_file):
        try:
            data_file = turle_file
            if turle_file.endswith('.bz2'):
                data_file = '/tmp/data.ttl'
                f = open(data_file, 'wb')
                f.write(bz2.BZ2File(turle_file).read())
                f.close()
             
            user = self.user.split(':')[0]
            pas = self.user.split(':')[1]
            r = requests.post(SERVER + "?graph-uri=" + uri,
                              auth=(user,pas), data=file(data_file,'rb').read())
            print r.status_code
        except:
            traceback.print_exc(file=sys.stdout)
            
    def upload_graph_beta(self, uri, turle_file):
        try:
            data_file = turle_file
            if turle_file.endswith('.bz2'):
                data_file = 'tmp/data.ttl'
                f = open(data_file, 'wb')
                f.write(bz2.BZ2File(turle_file).read())
                f.close()
                
            # Upload the new data    
            c = pycurl.Curl()
            c.setopt(c.VERBOSE, 1)
            c.setopt(c.URL, SERVER + "?graph-uri=" + uri)
            header = [ 'Content-Type:text/turtle']
            c.setopt(c.USERPWD, self.user)
            #c.setopt(c.POST, 1)
            c.setopt(c.UPLOAD, 1)
            c.setopt(c.HTTPPOST, [("file", (pycurl.FORM_FILE, data_file))])
            header = [ 'Content-Type:multipart/form-data', 'Expect: ']
            c.setopt(c.HTTPHEADER, header);
            #c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
            filesize = os.path.getsize(data_file)
            c.setopt(c.POSTFIELDSIZE, filesize)
            fin = open(data_file, 'rb')
            c.setopt(c.READFUNCTION, fin.read)
            c.perform()
            print c.getinfo(pycurl.HTTP_CODE), c.getinfo(pycurl.EFFECTIVE_URL)
            c.close()
        except:
            traceback.print_exc(file=sys.stdout)
            
    def upload_graph2(self, uri, turle_file):            
        # Upload the new data    
        c = pycurl.Curl()
        c.setopt(c.VERBOSE, 1)
        c.setopt(c.URL, SERVER + "?graph-uri=" + uri)
        c.setopt(c.USERPWD, self.user)
        c.setopt(c.POST, 1)
        #c.setopt(c.UPLOAD, 1)
        #header = [ 'Content-Type:multipart/form-data', 'Expect: ']
        header = [ 'Content-Type:text/turtle', 'Content-Encoding:gzip']
        c.setopt(c.HTTPHEADER, header);
        filesize = os.path.getsize(turle_file)
        c.setopt(c.POSTFIELDSIZE, filesize)
        fin = open(turle_file, 'rb')
        c.setopt(c.READFUNCTION, fin.read)
        #c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
        c.perform()
        c.close()
        
if __name__ == '__main__':
    data = "/tmp/data.ttl.bck"
    graph = "urn:graph:update:test:put"
    pusher = Pusher()
    #pusher.clean_graph(graph)
    pusher.upload_graph(graph, data)


# Push to OWLIM
# curl -X POST -H "Content-Type:application/x-turtle" -T config.ttl http://localhost:8080/openrdf-sesame/repositories/SYSTEM/rdf-graphs/service?graph=http://example.com#g1
# https://github.com/LATC/24-7-platform/blob/master/latc-platform/console/src/main/python/upload_files.py
# http://owlim.ontotext.com/display/OWLIMv52/OWLIM+FAQ

# Push to Virtuoso
# http://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtGraphProtocolCURLExamples

