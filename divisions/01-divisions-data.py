
# coding: utf-8

# # Votes (Divisions) in the Oireachtas: 01 - Data
# 
# An API is now available, see https://data.oireachtas.ie/ for documentation.

# In[1]:

# Commonly useful notebook functions:
import sys
sys.path.insert(0,'../lib')
from common import *


# In[2]:

# Save json from https://api.oireachtas.ie/
# Max results available for paging through with limit & skip is 10,000 so splitting by years works best
def save_page(year, overwrite=False):
    save_page_year('divisions', year, overwrite)
    
# Entire dataset: 11475 as of 15-12-2017, starts at 1922
years = range(1922, datetime.now().year + 1)
process_with_progress(save_page, years)
print()


# In[3]:

# To update after loading everything already: Grab the latest year and overwrite it:
save_page(datetime.now().year, True)


# In[7]:

# Explore an example year of results (2002 has very few votes for some reason):
with lzma.open('data/year/2002.json.xz', 'rb') as f:
    preview = json.loads(f.read().decode('utf-8'))


# In[8]:

RenderJSON(preview) # Preview all


# In[9]:

RenderJSON(preview['results'][0]['division']) # Preview single vote


# In[4]:

# Load Data from a single file and return dataframe
def divisions_df(fname):
    with lzma.open(fname, 'rb') as f:
        data = json.loads(f.read().decode('utf-8'))
        #print(fname, len(data['results']))
        
        # We don't care about the "head" part for now, just "results"
        records = json.dumps([r['division'] for r in data['results']])
        df = pd.read_json(records, orient='records', dtype=False)
        return df


# In[5]:

# Process all the files and stick them together in a single dataframe:
files = glob('data/year/*.json.xz')

data_frames = process_with_progress(divisions_df, files)
df_raw = pd.concat(data_frames, axis=0)


# In[6]:

# Not all the columns are useful, we can transform and drop the redundant ones:
df_raw.sample(3)


# In[7]:

flatten_columns = ['chamber','debate','house','subject'] # deal with tallies later
df_raw = flatten_dataframe_columns(df_raw, flatten_columns)

# category is always "Division", isBill is always False, not used in "Divisions"
# debate.formats.pdf, subject.uri are always empty for this data
df_raw.drop(['category','isBill','debate.formats.pdf','subject.uri'], axis=1, inplace=True)


# In[8]:

df_raw.sample(3)


# In[9]:

describe_with_top_n(df_raw, exclude_columns=['tallies'], n=50)


# In[10]:

df_raw.to_pickle('divisions.p.xz', compression='xz') # gzip is faster, but xz makes much smaller files for git


# In[ ]:



