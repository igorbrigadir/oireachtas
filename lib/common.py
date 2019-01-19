# Useful common imports and functions for notebooks:

# IO / misc:
import os
import sys
import uuid
import json
import lzma
import requests
from glob import glob
from datetime import datetime

from multiprocessing import Pool

# Pandas
import pandas as pd
from pandas.io.json import json_normalize
# Show More columns and rows in pandas
pd.set_option("display.max_columns",50)
pd.set_option("display.max_rows",1000)

# Notebook display
from IPython.display import display_javascript, display_html, display, Markdown

# Notebook Style
#from IPython.core.display import HTML
#with open(sys.path[0]+'/notebook.css') as f:
#    HTML(f.read())

# Progress bars
import tqdm
from tqdm import tqdm_notebook


# Max results available for paging through with limit & skip is 10,000 so splitting by years works best
def save_page_year(page, year, overwrite=False):
    fname = "data/year/{}.json.xz".format(year)
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
        results = list(tqdm_notebook(p.imap(func, iterable), total=len(iterable)))
    return results

# Print Markdown String
def print_markdown(string):
    display(Markdown(string))

# Render Json preview in a collapsible tree:
class RenderJSON(object):
    def __init__(self, json_data):
        if isinstance(json_data, dict):
            self.json_str = json.dumps(json_data)
        else:
            self.json_str = json
        self.uuid = str(uuid.uuid4())

    def _ipython_display_(self):
        js = """
          require(["https://rawgit.com/caldwell/renderjson/master/renderjson.js"], function() {
          renderjson.set_show_to_level(1)
          renderjson.set_sort_objects(true)
          renderjson.set_icons('▶', '▼')
          document.getElementById('%s').appendChild(renderjson(%s))
        });
        """ % (self.uuid, self.json_str)
        css = """
          .renderjson a { text-decoration: none; }
          .renderjson .disclosure { color: grey; text-decoration: none; }
          .renderjson .syntax { color: grey; }
          .renderjson .string { color: darkred; }
          .renderjson .number { color: darkcyan; }
          .renderjson .boolean { color: blueviolet; }
          .renderjson .key    { color: darkblue; }
          .renderjson .keyword { color: blue; }
          .renderjson .object.syntax { color: lightseagreen; }
          .renderjson .array.syntax  { color: orange; }
          """
        display_html('<style>{}</style><div id="{}" style="height: 100%; width:100%;"></div>'.format(css, self.uuid), raw=True)
        display_javascript(js, raw=True)

# Output report with .describe() and .value_counts() with missing counts
def describe_with_top_n(df, include_columns=[], exclude_columns=[], n=10):
    # Count Missing or null data
    missing_data = df.applymap(lambda x: not x or pd.isnull(x)).sum()

    if (len(include_columns) == 0):
        columns = df.columns
    else:
        columns = include_columns
    # All columns in dataframe, excluding exclude_columns
    for column in [str(column) for column in columns if column not in set(exclude_columns)]:
        print_markdown('----------')
        print_markdown('# '+ column)

        missing = pd.Series(data=missing_data[column], index=[column], name='missing')

        print_markdown('### ' + " Stats:")
        display(pd.DataFrame(df[column].describe()).append(missing))

        print_markdown('### ' + " Top N:")
        display(pd.DataFrame(df[column].value_counts()).head(n))

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
