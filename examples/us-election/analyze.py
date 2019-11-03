import scipy.stats as ss

from splitgraph.core.repository import Repository
from splitgraph.ingestion.pandas import sql_to_df

# Load the dataset we created into Pandas
image = Repository("", "qoz_vote_fraction").images["latest"]
df = sql_to_df("SELECT * FROM qoz_vote_fraction", image=image, use_lq=True)
print(df)

# Is there a correlation between the Trump vote fraction and the fraction of
# QOZ-qualified tracts in every county?
print(ss.linregress(df["trump_vote_fraction"], df["qoz_tract_fraction"]))
