
import sys
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd 


chl = sys.argv[1]
vid=sys.argv[2]
title = sys.argv[3]

def main():
    flag_db = 0
    print("Start: Aggregation")

    client = MongoClient("mongodb+srv://<credential>@cluster0.g7t88xt.mongodb.net/?retryWrites=true&w=majority")
    mydatabase = client['livechat_raw']
    collection = mydatabase[chl]

    df=pd.DataFrame()
    c=0
    for i in collection.find({"vid":vid}):
        t = i['datetime']
        if c == 0:
            hh=int(t[11:13]); mm=int(t[14:16])
        
        tt=datetime.strptime(t,'%Y-%m-%d %H:%M:%S')
        data = [[1,tt]]
        #data = [[i['neg'],i['neu'],i['pos'],i['compound'],tt]]
        if c > 0:
            df2 = pd.DataFrame(data, columns=['count','time'])
            #print("append")
            df = pd.concat([df, df2], axis=0)
        else:
            df = pd.DataFrame(data, columns=['count','time'])
        
        c += 1
    
    group_user_count  = df.groupby(pd.Grouper(key='time', axis=0, freq='1min'))
    
    result = group_user_count[['count']].sum()
    df_re = result[['count']].reset_index(drop=True)

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

    len_data = df_re['count'].count()

    client_sm = MongoClient("mongodb+srv://<credential>@cluster0.g7t88xt.mongodb.net/?retryWrites=true&w=majority")
    livechat_mart_db = client_sm['livechat_mart']
    _group_channel=f"Engagement_bytime_{chl}"
    print(_group_channel)
    livechat_collection = livechat_mart_db[_group_channel]

    for i in range(0,len_data):
        re = df_re.iloc[i,:]
        #record={"vid":re['vid'],"datetime":re['time'],"neg":re['neg'],"neu":re['neu'],"pos":re['pos'],"compound":re['compound']}
        record={"time":re['time'],"vid":vid,"count":int(re['count'])}
        if flag_db == 0:
            print(record)
        else:
            livechat_collection.insert_one(record)

    client_sm = MongoClient("mongodb+srv://<credential>@cluster0.g7t88xt.mongodb.net/?retryWrites=true&w=majority")
    livechat_mart_db = client_sm['livechat_mart']
    _group_channel=f"Engagement_bytime_{chl}"
    livechat_collection = livechat_mart_db[_group_channel]

    client_sm = MongoClient("mongodb+srv://<credential>@cluster0.g7t88xt.mongodb.net/?retryWrites=true&w=majority")
    livechat_mart_db = client_sm['livechat_mart']
    sentiment_group_channel=f"Sentiment_{chl}"
    print(sentiment_group_channel)
    livechat_collection = livechat_mart_db[sentiment_group_channel]
    ret = livechat_collection.aggregate([{'$match':{'vid':vid}},{'$group': {'_id': '$aname','count':{'$sum':1}}}, { '$sort' : {'count':-1 }} ])

    df=pd.DataFrame()
    c=0
    for i in ret:
        #print(i)
        data = [[i['_id'],i['count']]]
        if c > 0:
            df2 = pd.DataFrame(data, columns=['aname','count'])
            #print("append")
            df = pd.concat([df, df2], axis=0)
        else:
            df = pd.DataFrame(data, columns=['aname','count'])
    
        c += 1
    df.reset_index(drop=True)

    _group_channel=f"Engegement_count_by_name_{chl}"
    livechat_collection = livechat_mart_db[_group_channel]

    len_data=df['count'].count()
    c=0
    cc=0
    for i in range(0,len_data):
        data = df.iloc[i,:]
        record = {'aname':data['aname'],'vid':vid,'count':int(data['count'])}
        #print(record)
        if flag_db == 0:
            print(record)
        else:
            livechat_collection.insert_one(record)

        if c >=250:
            c=0
            print("%.2f " %(100*cc/len_data))
        
        c += 1
        cc += 1

    sentiment_group_channel=f"Sentiment_{channel}"
    print(sentiment_group_channel)
    livechat_collection = livechat_mart_db[sentiment_group_channel]

    ret = livechat_collection.aggregate([{'$match':{'aname':{'$in':topname_list},'vid':vid}}])
    df_top=pd.DataFrame()
    c=0
    for i in ret:
        #print(i)
        t = i['datetime']
        if c == 0:
            hh=int(t[11:13]); mm=int(t[14:16])
        
        tt=datetime.strptime(t,'%Y-%m-%d %H:%M:%S')

        data = [[i['vid'],tt,i['aname'],1]]
        if c > 0:
            df_top2 = pd.DataFrame(data, columns=['vid','datetime','aname','count'])
            #print("append")
            df_top = pd.concat([df_top, df_top2], axis=0)
        else:
            df_top = pd.DataFrame(data, columns=['vid','datetime','aname','count'])
    
        c += 1

    df_top = df_top.reset_index(drop=True)

    group_user_count  = df_top.groupby(pd.Grouper(key='datetime', axis=0, freq='5min'))
    #px.scatter(group_user_count,x="datetime",y="aname")

    ddf = pd.DataFrame()
    for i in group_user_count:
        t = i[0]
        dd = i[1]
        #print(f"{t} -> {dd['vid'].count()}")
    
        for j in range(0,dd['vid'].count()):
            data=[[t,dd.iloc[j,2],1]]
            #print(data)
            ddf2 = pd.DataFrame(data,columns=['time',"name","count"])
            ddf = pd.concat([ddf, ddf2], axis=0)
    
    result = ddf.groupby(['name','time'])['count'].count().reset_index()

    livechat_mart_db = client_sm['livechat_mart']
    livechat_collection = livechat_mart_db[sentiment_group_channel]

    _group_channel=f"Engegement_group_by_time_count_name_{channel}"
    livechat_collection = livechat_mart_db[_group_channel]
    len_data=result['count'].count()
    c=0
    cc=0
    for i in range(0,len_data):
        data = result.iloc[i,:]
        record = {'aname':data['name'],'vid':vid,'time':data['time'],'count':int(data['count'])}
        #print(record)
        if flag_db == 0:
            print(record)
        else:
            livechat_collection.insert_one(record)
        
        if c >=250:
            c=0
            print("%.2f " %(100*cc/len_data))
        
        c += 1
        cc += 1


    


    return(0)

if __name__ == "__main__":
    main()