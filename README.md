
# Integrator
**Copyright**: VU University Amsterdam, DANS

**License**: [LGPL v3.0](http://www.gnu.org/licenses/lgpl.html)

## Integration pipeline

This the data integration pipeline for the project [CEDA_R](http://www.cedar-project.nl/)

![Pipeline](https://raw.githubusercontent.com/CEDAR-project/Integrator/master/pipeline.png "Overview of the pipeline")

## Data

* Dumps are available in the directory "data/output" of this project.
* A sparql end point runs at [http://lod.cedar-project.nl/cedar/sparql](http://lod.cedar-project.nl/cedar/sparql)

## Mappings

The file format for the mapping/rules input files must be Excel, and all mappings must be specified in the first three columns of the first sheet, indicating the context, the string, and the target mapping. Examples are available in the `mapping` directory at https://github.com/CEDAR-project/DataDump-mini-vt

## Funding

The Integrator has been developed with funds from the Royal Dutch Academy of Arts and Sciences (<a href="http://www.knaw.nl/" target="_blank">KNAW</a>) and the Dutch National programme <a href="http://www.commit-nl.nl/about-commit" target="_blank">COMMIT</a>. For more information, learn about the <a href="http://www.ehumanities.nl/" target="_blank">eHumanities Group</a> and <a href="http://www.cedar-project.nl/" target="_blank">CEDAR</a>.
