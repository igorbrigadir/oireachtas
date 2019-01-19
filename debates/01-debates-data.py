
# coding: utf-8

# # Debates in the Oireachtas: 01 - Data
# 

# In[1]:

# Commonly useful notebook functions:
import sys
sys.path.insert(0,'../lib')
from common import *


# In[3]:

def save_page(year, overwrite=False):
    save_page_year('debates', year, overwrite)

# Entire dataset: 19349 as of 15-12-2017, starts at 1919
years = range(1919, datetime.now().year + 1)
process_with_progress(save_page, years)
print()


# In[ ]:



