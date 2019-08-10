# import scraperwiki
# import lxml.html
from sqlalchemy import create_engine
import os
import sys
import uuid
import json
import lzma
import requests
from glob import glob
from datetime import datetime
from multiprocessing import Pool
import pandas as pd
from pandas.io.json import json_normalize

#import tqdm
from tqdm import tqdm

# Max results available for paging through with limit & skip is 10,000 so splitting by years works best
def save_page_year(page, year, overwrite=False):
    fname = "data/{}/{}.json.xz".format(page,year)
    url = "https://api.oireachtas.ie/v1/{}?date_start={}&date_end={}&limit=10000".format(page, year, year+1)
    cache_url(url, fname)

# Save a given url as a file
def cache_url(url, fname, overwrite=False):
    if os.path.isfile(fname) and not overwrite:
        return

    data = requests.get(url).content
    with lzma.open(fname, 'wb') as f:            
        f.write(data)

# Show Progress bar and perform a function on an iterable in parallel:
def process_with_progress(func, iterable):
    with Pool(8) as p:
        results = list(tqdm(p.imap(func, iterable), total=len(iterable)))
    return results

# Preprocess:

# Flatten json / dict values in a column
def flatten_column(df, column):
    tmp_df = json_normalize([r for r in df[column]])
    tmp_df.rename(columns=lambda k: column+'.'+k, inplace=True)
    tmp_df.index = df.index
    return tmp_df

def flatten_dataframe_columns(df, columns):
    df_expanded = pd.concat([flatten_column(df, column) for column in columns], axis=1)
    df.drop(columns, axis=1, inplace=True) # remove flattened originally json columns
    df = pd.concat([df_expanded, df], axis=1)
    return df

""
# Debates:

def save_debates_page(year, overwrite=False):
    save_page_year('debates', year, overwrite)

# Entire dataset: 19349 as of 15-12-2017, starts at 1919
years = range(1919, datetime.now().year + 1)
print("Debates Pages:")
process_with_progress(save_debates_page, years)

# Update this year
save_debates_page(datetime.now().year, True)

# Load Data from a single file and return dataframe
def debates_df(fname):
    try:
        with lzma.open(fname, 'rb') as f:
            data = json.loads(f.read().decode('utf-8'))
            #print(fname, len(data['results']))
            
            # We don't care about the "head" part for now, just "results"
            records = json.dumps([r['debateRecord'] for r in data['results']])
            df = pd.read_json(records, orient='records', dtype=False)
            return df
    except:
        print("Failed Debates: ", fname)
        return pd.DataFrame()

# Process all the files and stick them together in a single dataframe:
files = glob('data/debates/*.json.xz')

print("Debates Data:")
data_frames = process_with_progress(debates_df, files)
df_raw = pd.concat(data_frames, axis=0, sort=True)

df_raw.drop(['debateSections'], axis=1, inplace=True)

flatten_columns = ['formats','chamber','counts', 'house'] # deal with debateSections later
df_raw = flatten_dataframe_columns(df_raw, flatten_columns)

df_raw.to_pickle('data/debates.p.xz', compression='xz') # gzip is faster, but xz makes much smaller files for git
print("Debates Saved.")

""
# Divisions

# Save json from https://api.oireachtas.ie/
# Max results available for paging through with limit & skip is 10,000 so splitting by years works best
def save_divisions_page(year, overwrite=False):
    save_page_year('divisions', year, overwrite)

print("Divisions Pages.")    
# Entire dataset: 11475 as of 15-12-2017, starts at 1922
years = range(1922, datetime.now().year + 1)
process_with_progress(save_divisions_page, years)
print()

# To update after loading everything already: Grab the latest year and overwrite it:
save_divisions_page(datetime.now().year, True)

# Load Data from a single file and return dataframe
def divisions_df(fname):
    with lzma.open(fname, 'rb') as f:
        data = json.loads(f.read().decode('utf-8'))
        #print(fname, len(data['results']))
        
        # We don't care about the "head" part for now, just "results"
        records = json.dumps([r['division'] for r in data['results']])
        df = pd.read_json(records, orient='records', dtype=False)
        return df

# Process all the files and stick them together in a single dataframe:
files = glob('data/divisions/*.json.xz')
print("Divisions Data.")
data_frames = process_with_progress(divisions_df, files)
df_raw = pd.concat(data_frames, axis=0, sort=True)

flatten_columns = ['chamber','debate','house','subject'] # deal with tallies later
df_raw = flatten_dataframe_columns(df_raw, flatten_columns)

# category is always "Division", isBill is always False, not used in "Divisions"
# debate.formats.pdf, subject.uri are always empty for this data
df_raw.drop(['category','isBill','debate.formats.pdf','subject.uri'], axis=1, inplace=True)

df_raw.to_pickle('data/divisions.p.xz', compression='xz')
print("Divisions Saved.")

""
# Legislation: TODO

print("Legislation Pages:")

def save_legislation_page(year, overwrite=False):
    save_page_year('legislation', year, overwrite)

# Entire dataset: 4637 as of 17-12-2017, starts at 1922
years = range(1922, datetime.now().year + 1)
process_with_progress(save_legislation_page, years)
print()

save_legislation_page(datetime.now().year, True)

""
# Questions: TODO, hits 10,000 limit, needs better paging.

print("Questions Pages:")

def save_questions_page(year, overwrite=False):
    save_page_year('questions', year, overwrite)

# Entire dataset: 443,950 as of 17-12-2017, starts at 1990
years = range(1990, datetime.now().year + 1)
process_with_progress(save_questions_page, years)
print()

save_questions_page(datetime.now().year, True)


""
# Write Data to tables:

db = create_engine('sqlite:///data.sqlite')

print("Metadata SQL:")

df_parties = pd.read_pickle('metadata/parties.p.xz')
df_parties.to_sql('parties', db, if_exists='append')

df_houses = pd.read_pickle('metadata/houses.p.xz')
df_houses.to_sql('houses', db, if_exists='append')

df_constituencies = pd.read_pickle('metadata/constituencies.p.xz')
df_constituencies.to_sql('constituencies', db, if_exists='append')

print("Debates SQL:")

df_debates = pd.read_pickle('data/debates.p.xz')
df_debates.to_sql('debates', db, if_exists='append')

#print("Divisions SQL:")
#df_divisions = pd.read_pickle('data/divisions.p.xz')
#df_divisions.to_sql('divisions', db, if_exists='append')
