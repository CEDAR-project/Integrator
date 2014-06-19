#!/usr/bin/python2
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib import ConjunctiveGraph, Namespace, Literal, RDF, RDFS, BNode, URIRef, XSD, Variable
from rdflib.plugins.sparql import prepareQuery
import re
import uuid
import rdflib
import bz2
import os.path
 
# Section: Integrity constraints
# check that no observation has two jobs, two cities, etc...
class SectionIntegrity(object):
	pass

# Section: Overview of dimensions
# how many observations have city, job, ...
class SectionDimensions(object):
	pass

# Section: Number of observations
# mapped (at least one harmonized dimension), ignored ("totaal"), lost
class SectionObservations(object):
	pass

# Section: Coding efficiency
# tell how many cities where not guessed, how many jobs are not mapped, etc 
class SectionCodes(object):
	pass

# everything listed in tables, one harmonized cube per row
# Report package
class Report(object):
	def __init__(self):
		self.cubes = [cube.strip() for cube in open('cubes.txt')]
		pass
	
	def go(self):
		integrity = SectionIntegrity();
		dimensions = SectionDimensions();
		observations = SectionObservations();
		codes = SectionCodes();

	def section_integrity(self):
		'''
		This section looks at integrity constraints
		'''
		pass
		
if __name__ == '__main__':
	report = Report()
	report.go()
	