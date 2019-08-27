import pandas as pd
import datetime
import json
import csv


#convert the date from string to datetime python object 
sub=pd.read_csv('pa_subtitles.csv')

for i in range(sub.shape[0]//7):
    if i%100==0:
        print(i)
    for j in range(7):
        sub['start'][i+j*15091] = datetime.datetime.strptime(sub['start'][i+j*15091], '%H:%M:%S.%f').time() 
        sub['end'][i+j*15091]=datetime.datetime.strptime(sub['end'][i+j*15091], '%H:%M:%S.%f').time() 
        sub['date'][i+j*15091]=datetime.datetime.strptime(sub['date'][i+j*15091], '%Y-%m-%d').date()
# sub.to_csv('pa_subtitles.csv',mode='a',index=False,header=False)
sub.to_hdf('pa_subtitles.hdf', key='abc')

#read and convert the json file
tweets = []
for line in open('INA_subtitles.json', 'r',encoding="utf-8"):
    tweets.append(json.loads(line))
    


row = ['content', 'start', 'end','channel','lang','date','duration']
dataf=list()
df=pd.DataFrame()

for i in range(len(tweets)):

    if i%10000==0:
        print(i)


    for j in range(len(tweets[i]['segment']['text'])):
        date_time_str=tweets[i]['segment']['text'][j]['event'][0]['startDate']
        row[1]=datetime.datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S,%f').time()
        enddate=tweets[i]['segment']['text'][j]['event'][0]['endDate']
        row[2]=datetime.datetime.strptime(enddate, '%Y-%m-%dT%H:%M:%S,%f').time()
        row[0]=tweets[i]['segment']['text'][j]['value']
        row[3]=tweets[i]['segment']['text'][j]['event'][0]['agent'][0]['identifier']
        row[4]=tweets[i]['segment']['text'][j]['lang']
        row[5]=datetime.datetime.strptime(enddate, '%Y-%m-%dT%H:%M:%S,%f').date()
        row[6]=(datetime.datetime.strptime(enddate, '%Y-%m-%dT%H:%M:%S,%f')-datetime.datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S,%f')).total_seconds()
        dataf.append([row[0],row[1],row[2],row[3],row[4],row[5],row[6]])

dat= pd.DataFrame(dataf, columns=['content', 'start', 'end','channel','lang','date','duration'])
dat.to_hdf('INA_subt.hdf', key='abc')
