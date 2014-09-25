prefix owl: <http://www.w3.org/2002/07/owl#>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix rules: <http://rules.example.org/resource/>
prefix prov: <http://www.w3.org/ns/prov#>
prefix harmonizer: <http://example.org/ns/harmonizer#>
prefix sdmx-code: <http://purl.org/linked-data/sdmx/2009/code#>
prefix oa: <http://www.w3.org/ns/openannotation/core/>
prefix qb: <http://purl.org/linked-data/cube#>
prefix dcat: <http://www.w3.org/ns/dcat#>
prefix tablink: <http://example.org/ns/tablink#>
prefix sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
prefix np: <http://www.nanopub.org/nschema#>
prefix cedar: <http://cedar.example.org/resource/>
prefix dcterms: <http://purl.org/dc/terms/>
prefix skos: <http://www.w3.org/2004/02/skos/core#>
 
# Count the number of raw datasets

PREFIX qb:  <http://purl.org/linked-data/cube#>
SELECT (Count(*) AS ?total) WHERE
{ GRAPH <urn:graph:cedar:raw-rdf> { 
?s a qb:DataSet 
} }

or

PREFIX qb:  <http://purl.org/linked-data/cube#>
SELECT (Count(distinct ?s) AS ?total) FROM <urn:graph:cedar:raw-rdf> WHERE
{  ?s a qb:DataSet  }

# Get the number of data set per source
SELECT distinct ?location (Count(distinct ?d) AS ?total) FROM <urn:graph:cedar:raw-rdf> WHERE {
?d a qb:DataSet.
?d prov:wasDerivedFrom ?src.
?src a dcat:DataSet.
?src dcat:distribution ?file.
?file dcterms:accessURL ?location.
} group by ?location

# Get number of datasets per source
prefix qb: <http://purl.org/linked-data/cube#>
prefix prov: <http://www.w3.org/ns/prov#>
prefix dcat: <http://www.w3.org/ns/dcat#>
prefix dcterms: <http://purl.org/dc/terms/>
SELECT distinct ?title (Count(distinct ?d) AS ?total) FROM <urn:graph:cedar:raw-rdf> WHERE {
	?d a qb:DataSet.
	?d prov:wasDerivedFrom ?src.
	?src a dcat:DataSet.
	?src dcat:distribution [ dcterms:title ?title ].
} group by ?title order by ?title


# Apply rule
prefix harmonizer: <http://bit.ly/cedar-harmonizer#>
prefix qb: <http://purl.org/linked-data/cube#>

construct {
?obs a qb:Observation.
?obs qb:dataSet <http://lod.cedar-project.nl:8888/cedar/resource/BRT_1930_07_S3_S0>.
?obs ?dim ?val.
} where {
?obs ?rawdim ?rawval.
?rule a harmonizer:SetValue .
?rule harmonizer:targetDimension ?rawdim .
?rule harmonizer:targetValue ?rawval .
?rule harmonizer:targetDataset <http://lod.cedar-project.nl:8888/cedar/resource/BRT_1930_07_S3_S0>.
?rule harmonizer:dimension ?dim .
?rule harmonizer:value ?val .
}

# Second model
## Get number of sheets per source
prefix qb: <http://purl.org/linked-data/cube#>
prefix prov: <http://www.w3.org/ns/prov#>
prefix dcat: <http://www.w3.org/ns/dcat#>
prefix dcterms: <http://purl.org/dc/terms/>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT distinct ?title (Count(distinct ?sheet) AS ?total) FROM <urn:graph:cedar:raw-data> WHERE {
	?dataset a dcat:DataSet.
	?dataset prov:wasDerivedFrom [ dcat:distribution [ rdfs:label ?title ] ].
	?dataset dcterms:hasPart ?sheet.
} group by ?title order by ?title

