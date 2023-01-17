
import sys
from pymongo import MongoClient
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from tqdm.notebook import tqdm
from datetime import datetime, timedelta
import pandas as pd 

chl = sys.argv[1]
vid=sys.argv[2]
title = sys.argv[3]

def main():
    flag_db = 0
    print("Start: Sentiment")
    
    sia = SentimentIntensityAnalyzer()

    client = MongoClient("mongodb+srv://<credential>@cluster0.g7t88xt.mongodb.net/?retryWrites=true&w=majority")
    mydatabase = client['livechat_raw']
    collection = mydatabase[chl]

    client_sm = MongoClient("mongodb+srv://<credential>@cluster0.g7t88xt.mongodb.net/?retryWrites=true&w=majority")
    livechat_mart_db = client_sm['livechat_mart']
    collection_out = f"Sentiment_{chl}"
    livechat_collection = livechat_mart_db[collection_out]

    ret = collection.find({"vid":vid})

    cc = 0
    c=0
    for i in ret:
        result = sia.polarity_scores(i['msg'])
        #print(f"{i['datetime']},{i['aname']},{result['neg']},{result['neu']},{result['pos']},{result['compound']}")
        record={"vid":i['vid'],"datetime":i['datetime'],"aname":i['aname'],"neg":result['neg'],"neu":result['neu'],"pos":result['pos'],"compound":result['compound']}
        if flag_db == 0:
            print(record)
        else:
            livechat_collection.insert_one(record)
    
        cc +=1
        if cc > 200:
            print(f"{c}")
            cc = 0
        c += 1

    # For Sentiment_bytime_

    ret = livechat_collection.find({"vid":vid})

    df=pd.DataFrame()
    c=0
    for i in ret:
        t = i['datetime']
        if c == 0:
            hh=int(t[11:13]); mm=int(t[14:16])
        
        tt=datetime.strptime(t,'%Y-%m-%d %H:%M:%S')
        data = [[i['neg'],i['neu'],i['pos'],i['compound'],tt]]
        if c > 0:
            df2 = pd.DataFrame(data, columns=['neg', 'neu','pos','compound','time'])
            #print("append")
            df = pd.concat([df, df2], axis=0)
        else:
            df = pd.DataFrame(data, columns=['neg', 'neu','pos','compound','time'])
        
        c += 1
    
    ii = df.groupby(pd.Grouper(key='time', axis=0, freq='1min'))
    result = ii[['neg','pos','neu','compound']].sum()
    df_re = result[['neg','neu','pos','compound']].reset_index(drop=True)

    df_t = pd.DataFrame()
    l = result.index
    c=0
    for index in l:
        #print(index)
        if c > 0:
            df2 = pd.DataFrame([index], columns=['time'])
            #print("append")
            df_t = pd.concat([df_t, df2], axis=0)
        else:
            df_t = pd.DataFrame([index], columns=['time'])
        
        c += 1
    
    df_re['time'] = df_t.reset_index(drop=True)
    len_data = df_re['neg'].count()
    aa=[]
    for i in range(0,len_data):
        aa.append(vid)
    
    df_vid=pd.DataFrame(aa,columns=["vid"])
    df_re['vid'] = df_vid

    for i in range(0,len_data):
        re = df_re.iloc[i,:]
        record={"vid":re['vid'],"datetime":re['time'],"neg":re['neg'],"neu":re['neu'],"pos":re['pos'],"compound":re['compound']}
        if flag_db == 0:
            print(record)
        else:
            sentiment_group_channel=f"Sentiment_bytime_{channel}"
            livechat_collection = livechat_mart_db[sentiment_group_channel]
            livechat_collection.insert_one(record)




    return(0)

if __name__ == "__main__":
    main()