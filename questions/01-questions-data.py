
# coding: utf-8

# # Questions in the Oireachtas: 01 - Data
# 

# In[2]:

# Commonly useful notebook functions:
import sys
sys.path.insert(0,'../lib')
from common import *


# In[3]:

def save_page(year, overwrite=False):
    save_page_year('questions', year, overwrite)

# Entire dataset: 443,950 as of 17-12-2017, starts at 1990
years = range(1990, datetime.now().year + 1)
process_with_progress(save_page, years)
print()


# In[11]:

# Update
save_page(datetime.now().year, True)


# In[ ]:



