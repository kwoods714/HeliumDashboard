#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd 
import requests
import json
import numpy as np


# In[2]:


min_time= '2021-09-01'
max_time='2021-10-01'


# In[3]:


hotspotLists = pd.read_excel("/Users/kevinwoods/Desktop/Helium/hotspotlists.xlsx")
hotspotLists


# In[4]:


emritlist = hotspotLists.query("relationships=='Emrit'").reset_index(drop=True)
emritlist


# # API CALLS

# In[5]:


for index, row in emritlist.iterrows():
    response=requests.get('https://api.helium.io/v1/hotspots/{}/rewards/sum?min_time={}T13:14:46.577281&max_time={}T13:14:46.577281'.format(row['hotspot_address'],min_time,max_time))
    row['issueData']=response.json()
    
emritlist['issueData2'] = ""

for index, row in emritlist.iterrows():
    response=requests.get(f"https://api.helium.io/v1/hotspots/{row['hotspot_address']}/rewards/sum?min_time={min_time}T13:14:46.577281&max_time={max_time}T13:14:46.577281")
    row['issueData2']=response.json()
    
result = emritlist.to_json(orient="records")

parsed = json.loads(result)

hotspotlist_before_calculated_cols = pd.json_normalize(parsed)
hotspotlist_before_calculated_cols


# # Performs Row Operations

# In[6]:


hotspotlist_before_calculated_cols = hotspotlist_before_calculated_cols.assign(Our_Revenue = lambda x: (x['issueData2.data.total'] * 0.2))
hotspotlist_before_calculated_cols = hotspotlist_before_calculated_cols.assign(Emrit_Keeps = lambda x: (x['issueData2.data.total'] * 0.8))
hotspotlist_before_calculated_cols = hotspotlist_before_calculated_cols.assign(Tax_Amount = lambda x: (x['Our_Revenue'] * 0.2))
hotspotlist_before_calculated_cols = hotspotlist_before_calculated_cols.assign(Net_Income = lambda x: ((x['Our_Revenue'] - x['Tax_Amount']) / 2))
hotspotlist_before_calculated_cols = hotspotlist_before_calculated_cols.assign(Host_Income = lambda x: (x['Net_Income']))
hotspotlist_before_calculated_cols = hotspotlist_before_calculated_cols.assign(amount = lambda x: (x['Host_Income']))
hotspotlist_before_calculated_cols = hotspotlist_before_calculated_cols.assign(address = lambda x: (x['wallet_address']))

# Only looks at those who we have a wallet address for
hotspotlist_after_calculated_cols = hotspotlist_before_calculated_cols.query('wallet_address.notna()', engine='python').reset_index(drop=True)
hotspotlist_after_calculated_cols


# # Pays Hosts when amount is not Zero

# In[7]:


emritpayouts_beforezero = pd.pivot_table(hotspotlist_after_calculated_cols, values=['amount'], index=['host_name','address'], aggfunc=np.sum).reset_index().rename_axis(None,axis=1)
emritpayouts_afterzero = emritpayouts_beforezero.query('amount!=0').reset_index(drop=True)


# In[8]:


hotspotlist_after_calculated_cols[['issueData2.data.total','Our_Revenue', 'Emrit_Keeps', 'Tax_Amount', 'Net_Income', 'Host_Income','amount']].sum(axis = 0, skipna = True)


# In[9]:


hotspotlist_after_calculated_cols[['hotspot_name', 'host_name',
       'relationships',
       'issueData2.data.total',
       'Our_Revenue', 'Emrit_Keeps', 'Tax_Amount', 'Net_Income', 'Host_Income',
       'amount', 'address']]#.sum(axis = 0, skipna = True)


# # Pays Interns when amount is not Zero
# 

# In[10]:


emritpayouts_afterzero


# In[11]:


internlist = hotspotLists[['host_name','wallet_address','relationships']].query("relationships=='Intern'").reset_index(drop=True)
internlist =internlist.assign(address = lambda x: (x['wallet_address']))
internlist['amount']= hotspotlist_after_calculated_cols['Net_Income'].sum()/len(internlist)
internlist = internlist[['host_name','address','amount']]
internlist


# # Creates JSON file

# In[12]:


payout_json_file = emritpayouts_afterzero[['address','amount']].to_json(orient = 'records')
payout_json_file 

