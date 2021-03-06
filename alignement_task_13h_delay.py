import pandas as pd
import time as tme
from fuzzywuzzy import fuzz
import datetime
from datetime import  timedelta, time
from fuzzywuzzy import process


def add_delta(date,tme, delta):
   
    return (datetime.datetime.combine(date, tme) + 
            delta)
def minus_delta(date,tme, delta):
   
    return (datetime.datetime.combine(date, tme) - 
            delta)


#reading data
asr_out_fr2 = pd.read_hdf('remaining_asr_out_fr2.hdf')
ground_truth_fr2=pd.read_hdf('remaining_ground_truth_fr2.hdf')

# ground_truth_=ground_truth.sort_values(by='start', ascending=False)[['content','date','duration','start','end','channel']]
# asr_out_=asr_out.sort_values(by='start', ascending=True)[['content','date','duration','start','end','dataset']]
# #add the hour feature
# ground_truth_['hour_start']=[d.hour for d in ground_truth_['start']]
# asr_out_['hour_start']=[d.hour for d in asr_out_['start']]

# #add the full date feature (date + start time)
# asr_out_['full_date']=[datetime.datetime.combine(asr_out_['date'].loc[i], asr_out_['start'].loc[i]) for i in asr_out_.index]
# ground_truth_['full_date']=[datetime.datetime.combine(ground_truth_['date'].loc[i], ground_truth_['start'].loc[i]) for i in ground_truth_.index]

# ground_truth_['content']=ground_truth_['content']+" "
# asr_out_['content']=asr_out_['content']+" "


# ground_truth_=ground_truth_.reset_index()
# asr_out_=asr_out_.reset_index()
# #keep just the rows where the channel is FR2
# asr_out_fr2=asr_out_.where(asr_out_['dataset']=='fr2').dropna()
# ground_truth_fr2=ground_truth_.where(ground_truth_['channel']=='FR2').dropna()

#find the matching content 
print("starting the alignement...")

start_time = tme.time()


score=[]
index2=ground_truth_fr2.index
for i in range(len(index2)):
    T=asr_out_fr2['full_date']
    Tp=add_delta(ground_truth_fr2.loc[index2[i]]['date'],ground_truth_fr2.loc[index2[i]]['start'], datetime.timedelta(hours=2))
    Tm=minus_delta(ground_truth_fr2.loc[index2[i]]['date'],ground_truth_fr2.loc[index2[i]]['start'], datetime.timedelta(hours=2))
    cam_item=asr_out_fr2.where((T<Tp)&(T>Tm)).dropna()
    index1=cam_item.index
    if i%1000==0:
        print(i)
    for j in range(len(index1)):
        
        st1=ground_truth_fr2.loc[index2[i]]['content']
        st2=asr_out_fr2.loc[index1[j]]['content']
        score.append([index2[i],index1[j],fuzz.ratio(st1,st2)])

matched_text= pd.DataFrame(score, columns=['index_gt','index_asr','score'])

match_df=matched_text.sort_values('score').drop_duplicates(['index_gt'],keep='last')
match_df=matched_text.sort_values('score').drop_duplicates(['index_asr'],keep='last')

match_df.to_hdf('remaining_matched_text_fr2.hdf', key='abc')

print('end')
print("--- %s seconds ---" % (tme.time() - start_time))
