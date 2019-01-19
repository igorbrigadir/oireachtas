
# coding: utf-8

# # Oireachtas Metadata
# 
# Useful metadata for joining with other types of data:
# * `/houses` Houses
# * `/parties` Parties List
# * `/constituencies` Constituencies List

# In[1]:

# Commonly useful notebook functions:
import sys
sys.path.insert(0,'../lib')
from common import *


# In[5]:

# Load Data from a single file and return dataframe
def cache_metadata(path, chamber_id):
    path_url = "https://api.oireachtas.ie/v1/{}?chamber_id={}&limit=10000".format(path, chamber_id)
    fname = "data/{}/{}.json.xz".format(path, chamber_id.replace('/ie/oireachtas/house/','').replace('/','_'))
    cache_url(path_url, fname)


# In[8]:

cache_url("https://api.oireachtas.ie/v1/houses?limit=10000", "data/houses.json.xz")


# In[9]:

def house_df(fname):
    with lzma.open(fname, 'rb') as f:
        data = json.loads(f.read().decode('utf-8'))
        records = json.dumps([r for r in data['results']])
        df = pd.read_json(records, orient='records', dtype=False)
        df = flatten_dataframe_columns(df, ['house'])
        return df


# In[10]:

df_house = house_df("data/houses.json.xz")
df_house.to_pickle('houses.p.xz', compression='xz')


# In[12]:

# preview houses
with lzma.open('data/houses.json.xz', 'rb') as f:
    house_preview = json.loads(f.read().decode('utf-8'))
RenderJSON(house_preview)


# In[13]:

for chamber_id in tqdm_notebook(df_house['house.uri']):
    cache_metadata('constituencies', chamber_id)
    cache_metadata('parties', chamber_id)


# In[14]:

# preview constituency for dail
with lzma.open('data/constituencies/dail_32.json.xz', 'rb') as f:
    constituency_preview = json.loads(f.read().decode('utf-8'))
RenderJSON(constituency_preview['results']['house'])


# In[15]:

# preview panel for seanad
with lzma.open('data/constituencies/seanad_25.json.xz', 'rb') as f:
    constituency_preview = json.loads(f.read().decode('utf-8'))
RenderJSON(constituency_preview['results']['house'])


# In[16]:

def meta_df(fname, field1, field2):
    with lzma.open(fname, 'rb') as f:
        data = json.loads(f.read().decode('utf-8'))
        if 'results' not in data:
            print("No Records for", fname, '\n', data)
            return pd.DataFrame()
        records = json.dumps([r for r in data['results']['house'][field1]])
        df = pd.read_json(records, orient='records', dtype=False)
        df = flatten_dataframe_columns(df, [field2])
        return df


# In[17]:

df_parties = pd.concat([meta_df(fname, 'parties', 'party') for fname in glob('data/parties/*.json.xz')])
df_constituencies = pd.concat([meta_df(fname, 'constituenciesOrPanels', 'constituencyOrPanel') for fname in glob('data/constituencies/*.json.xz')])   

df_parties.to_pickle('parties.p.xz', compression='xz')
df_constituencies.to_pickle('constituencies.p.xz', compression='xz')


# In[18]:

describe_with_top_n(df_house)
describe_with_top_n(df_parties)
describe_with_top_n(df_constituencies)


# In[25]:




# In[ ]:



