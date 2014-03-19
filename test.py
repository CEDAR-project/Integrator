#!/usr/bin/python2
import rdflib
import bz2

if __name__ == '__main__':
    rulesFile = '/home/cgueret/Dropbox/Code/CEDAR/DataDump/rdf/VT_1899_07_H1_marked.ttl.bz2'
    rules = rdflib.ConjunctiveGraph()
    rules.load(bz2.BZ2File(rulesFile), format="turtle")
    print "Loaded %d triple rules from %s" % (len(rules), rulesFile)
    