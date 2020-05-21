# Joining multiple Splitgraph tables: 2016 US Election 

This example will:

* use a Splitfile to build a dataset that joins several datasets together:
  * [US Census](https://www.splitgraph.com/splitgraph/census) demographic data ([source](https://www.kaggle.com/muonneutrino/us-census-demographic-data/download))
  * Census tracts designated as [Qualified Opportunity Zones](https://www.splitgraph.com/splitgraph/qoz) ([source](https://www.cdfifund.gov/Documents/Designated%20QOZs.12.14.18.xlsx))
  * [2016 US Presidential Election precinct-level returns](https://www.splitgraph.com/splitgraph/2016_election/) ([source](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/LYWX3D))
* Run a quick linear regression against the resultant dataset to see if there is a
  correlation between the voting patterns in a given county and the fraction of QOZ-qualified
  census tracts in that county. 

## Running the example

Install this package with [Poetry](https://github.com/sdispater/poetry): `poetry install` 

Copy your .sgconfig file into this directory (it must contain API credentials to access
data.splitgraph.com). If you don't have them yet, take a look at the
[Splitgraph Cloud reference](https://www.splitgraph.com/docs/splitgraph_cloud/introduction) or register using `sgr cloud register`.

Then, run `../run_example.py example.yaml` and press ENTER when prompted to go through the steps.