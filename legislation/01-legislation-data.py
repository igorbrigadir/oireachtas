
# coding: utf-8

# # Legislation in the Oireachtas: 01 - Data
# 

# In[3]:

# Commonly useful notebook functions:
import sys
sys.path.insert(0,'../lib')
from common import *


# In[4]:

def save_page(year, overwrite=False):
    save_page_year('legislation', year, overwrite)

# Entire dataset: 4637 as of 17-12-2017, starts at 1922
years = range(1922, datetime.now().year + 1)
process_with_progress(save_page, years)
print()


# In[6]:

# Update
save_page(datetime.now().year, True)


# In[ ]:



