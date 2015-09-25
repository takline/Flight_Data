
# coding: utf-8

# In[1]:

import pandas as pd 
from subprocess import Popen, PIPE, STDOUT
import csv
from urllib2 import Request, urlopen
import json
from pandas.io.json import json_normalize
import datetime
import time


# In[2]:

df=pd.read_csv('/Users/xgl470/Downloads/dcah/DCAH_Competition_Data_VA_Flights.csv',encoding='utf-8', sep =',')


# In[3]:

def f(x):
    return  x[2] + x[3] 


# In[4]:

df['fligt_id']=df["FL_NUM"].map(str) + df["ORIGIN"]+ df["CRS_DEP_TIME"].map(str)+ df["FL_DATE"]


# In[5]:

flight_dict=df.set_index('fligt_id').to_dict()


# # setting up dict

# In[100]:

masterdict={}


# In[101]:

for x in flight_dict['ORIGIN']:
    y=flight_dict['ORIGIN'][x]
    masterdict[y]={}


# setting up a dict for each origin/destination

# In[102]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    masterdict[origin][dest]={}


# #finding the total # of flight records per origin/destination

# In[103]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    masterdict[origin][dest]['count']=0


# In[104]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    masterdict[origin][dest]['count']+=1


# finding the total travel time per origin/destination

# In[105]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    masterdict[origin][dest]['travel_time']=0


# In[106]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    if flight_dict['CANCELLED'][x]==1:
        travel_time=0
    elif flight_dict['DIVERTED'][x]==1:
        travel_time=flight_dict['CRS_ELAPSED_TIME'][x]
    else:
        travel_time=flight_dict['ACTUAL_ELAPSED_TIME'][x]
    masterdict[origin][dest]['travel_time']+=travel_time


# calculating experience factor. If a flight is delayed, negative score. If a flight leaves early, positive score. If it is cancelled, large negative score. Diversions also are pretty bad here

# In[107]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    masterdict[origin][dest]['experience']=0


# In[108]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    if flight_dict['CANCELLED'][x]==1:
        experience=-flight_dict['CRS_ELAPSED_TIME'][x]
    elif flight_dict['DIVERTED'][x]==1:
        experience=-flight_dict['DEP_DELAY_NEW'][x]-flight_dict['CRS_ELAPSED_TIME'][x]
    else:
        experience=-flight_dict['ARR_DELAY'][x]
    masterdict[origin][dest]['experience']+=experience


# # Now let's create an "experience score"

# In[111]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    masterdict[origin][dest]['experience_score']=0


# In[112]:

for x in flight_dict['DEST']:
    origin=flight_dict['ORIGIN'][x]
    dest=flight_dict['DEST'][x]
    experience=masterdict[origin][dest]['experience']
    total_flight_time=masterdict[origin][dest]['travel_time']
    score=experience/total_flight_time
    masterdict[origin][dest]['experience_score']=score


# In[130]:

unleveled_df=pd.DataFrame(pd.concat(map(pd.DataFrame, masterdict.values()), keys=masterdict.keys())).stack().reset_index()


# # format the dataframe

# In[158]:

count=unleveled_df[(unleveled_df["level_1"] =='count')]


# In[159]:

count = count.drop('level_1', 1)


# In[160]:

count.columns=['origin','desitination','count']


# In[161]:

experience=unleveled_df[(unleveled_df["level_1"] =='experience')]


# In[162]:

experience = experience.drop('level_1', 1)


# In[163]:

experience.columns=['origin','desitination','experience']


# In[164]:

experience_score=unleveled_df[(unleveled_df["level_1"] =='experience_score')]


# In[165]:

experience_score = experience_score.drop('level_1', 1)


# In[166]:

experience_score.columns=['origin','desitination','experience_score']


# In[174]:

travel_time =unleveled_df[(unleveled_df["level_1"] =='travel_time')]


# In[176]:

travel_time  = travel_time.drop('level_1', 1)


# In[177]:

travel_time .columns=['origin','desitination','travel_time']


# In[178]:

final=pd.merge(count, experience, on=['origin','desitination'], how='inner')


# In[179]:

final=pd.merge(final, travel_time, on=['origin','desitination'], how='inner')


# In[180]:

final=pd.merge(final, experience_score, on=['origin','desitination'], how='inner')


# In[182]:

final.to_csv('/Users/xgl470/Downloads/dcah/final_results.csv', sep =',')


# # 2nd Part: creating Json file for moving graph

# In[8]:

airlines=[]


# In[9]:

for x in flight_dict['CARRIER']:
    airlines.append(flight_dict['CARRIER'][x])


# In[ ]:

airlines=set(airlines)


# In[67]:

dates=[]


# In[68]:

for x in flight_dict['FL_DATE']:
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    dates.append(date)


# In[69]:

dates=list(set(dates))


# In[73]:

dates=sorted(dates)


# number of flights

# In[74]:

flight_count={}


# In[75]:

for x in airlines:
    flight_count[x]={}


# In[76]:

for x in flight_dict['CARRIER']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    flight_count[airline][date]=0


# In[77]:

for x in flight_dict['CARRIER']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    flight_count[airline][date]+=1


# In[26]:

number_of_flights={}


# In[80]:

for x in airlines:
    number_of_flights[x]=[]


# In[81]:

for x in airlines:
    for y in dates:
        to_append=[y,flight_count[x][y]]
        number_of_flights[x].append(to_append)


# negatives score

# In[89]:

scoring={}


# In[90]:

for x in airlines:
    scoring[x]={}


# In[91]:

for x in flight_dict['CARRIER']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    scoring[airline][date]=0


# In[93]:

for x in flight_dict['CARRIER']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    if flight_dict['CANCELLED'][x]==1:
        experience=-flight_dict['CRS_ELAPSED_TIME'][x]
    elif flight_dict['DIVERTED'][x]==1:
        experience=-flight_dict['DEP_DELAY_NEW'][x]-flight_dict['CRS_ELAPSED_TIME'][x]
    else:
        experience=-flight_dict['ARR_DELAY'][x]
    scoring[airline][date]+=experience


# total time

# In[98]:

total_time={}


# In[99]:

for x in airlines:
    total_time[x]={}


# In[100]:

for x in flight_dict['CARRIER']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    total_time[airline][date]=0


# In[101]:

for x in flight_dict['CARRIER']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    if flight_dict['CANCELLED'][x]==1:
        travel_time=0
    elif flight_dict['DIVERTED'][x]==1:
        travel_time=flight_dict['CRS_ELAPSED_TIME'][x]
    else:
        travel_time=flight_dict['ACTUAL_ELAPSED_TIME'][x]
    total_time[airline][date]+=travel_time


# final experience score

# In[103]:

final_exp={}


# In[104]:

for x in airlines:
    final_exp[x]={}


# In[105]:

for x in flight_dict['DEST']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    final_exp[airline][date]=0


# In[106]:

for x in flight_dict['DEST']:
    airline=flight_dict['CARRIER'][x]
    date=flight_dict['FL_DATE'][x]
    date=flight_dict['FL_DATE'][x]
    date=date[2:]
    date=date.replace('/15','')
    date=int(date)
    experience=scoring[airline][date]
    total_flight_time=total_time[airline][date]
    score=experience/total_flight_time
    final_exp[airline][date]=score


# In[109]:

negative_score={}


# In[110]:

for x in airlines:
    negative_score[x]=[]


# In[111]:

for x in airlines:
    for y in dates:
        to_append=[y,final_exp[x][y]]
        negative_score[x].append(to_append)


# create the final json file

# In[113]:

final_json=[]


# In[114]:

for x in airlines:
    new_dict={}
    new_dict['name']=x
    new_dict['number_of_flights']=number_of_flights[x]
    new_dict['negative_score']=negative_score[x]
    final_json.append(new_dict)


# In[120]:

import json
with open('/Users/xgl470/Downloads/dcah/movingchart/updates.json', 'w') as outfile:
    json.dump(final_json, outfile)


# In[ ]:




# In[ ]:



