# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta

from pymongo import MongoClient
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

external_stylesheets = ['bWLwgP.css']

app = Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP,dbc.icons.BOOTSTRAP])

client_sm = MongoClient("mongodb+srv://appuser:dXb2gIgAEXf3NA3E@cluster0.g7t88xt.mongodb.net/?retryWrites=true&w=majority")
livechat_mart_db = client_sm['livechat_mart']

livechat_collection = livechat_mart_db["meta"]
ret = livechat_collection.find({})
df_list=[]
c=0
for i in ret:
    data = [[i['ch_name'],i['vid'],i['title']]]
    #print(data)
    if c > 0:
        df_list2 = pd.DataFrame(data, columns=['ch_name', 'vid','title'])
        print("append")
        df_list = pd.concat([df_list, df_list2], axis=0)
        
    else:
        df_list = pd.DataFrame(data, columns=['ch_name', 'vid','title'])

    c += 1

livechat_collection = livechat_mart_db["Sentiment_DrDisRespect"]
ret = livechat_collection.aggregate([
                        { '$group': 
                            {
                                '_id': '$vid'
                                ,'Neg':{'$sum':"$neg"} 
                                ,'Neu':{'$sum':"$neu"}
                                ,'Pos':{'$sum':"$pos"}
                                ,'Compound':{'$sum':"$compound"}
                            }
                        }
])

#print(ret[0])
dd={}
for i in ret:
    dd[i['_id']] = i

livechat_collection = livechat_mart_db["Sentiment_TheBrokenMachine"]
ret = livechat_collection.aggregate([
                        { '$group': 
                            {
                                '_id': '$vid'
                                ,'Neg':{'$sum':"$neg"} 
                                ,'Neu':{'$sum':"$neu"}
                                ,'Pos':{'$sum':"$pos"}
                                ,'Compound':{'$sum':"$compound"}
                            }
                        }
])

for i in ret:
    dd[i['_id']] = i

data = dd['uuz78OBFCfw']
df = pd.DataFrame({
    "Sentimet": "Value",
    "Amount": [data['Neg'], data['Neu'], data['Pos'],data['Compound']],
    "City": ["Negative", "Nue", "Positive", "Compouse"]
})

def get_sentiment_by_vid(vid):
    return(dd[vid])


tabs_styles = {
    'height': '44px'
}

tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}

tab_sub_style = {
    'borderBottom': '1px solid #E6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_sub_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #E6d6d6',
    'backgroundColor': '#219DFF',
    'color': 'white',
    'padding': '6px'
}



#fig = px.bar(df, x="Fruit", y="Amount", color="City")
fig = px.bar(df, x="Sentimet", y="Amount", color="Sentimet", barmode="group")

app.layout = html.Div(children=[
    html.H1(children='First Person Shooter Games Live Chat'),

html.Div([
    html.Div(children='''
        First Person Shooter Games Live Chat.
    '''),
    dbc.Row([ 
        html.Div([
            dcc.Dropdown(
                df_list['title'].unique(),
                df_list['title'].iloc[0],
                id='crossfilter-xaxis-column',
            ),
        ],),
    ]),
    #html.Div([
    dbc.Row([  
        dcc.Tabs([
        dcc.Tab(label='Engagement', children=[
                dbc.Row([
                    dbc.Col([
                        html.Div(children=[
                            html.H3(id='Engagement_TotalReplay', style={'fontWeight': 'bold','color':'blue','font-size':'320%','text-align':'center'}),
                            html.H2('Total Comments', style={'paddingTop': '.3rem','fontWeight': 'bold','font-size':'150%','text-align':'center'}),
                        ])
                    ], className="three columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem' ,"background-color": "#fdccee"}),
                    dbc.Col([
                        html.Div(children=[
                            html.H3(id='Engagement_ave_min', style={'fontWeight': 'bold','color':'blue','font-size':'320%','text-align':'center'}),
                            html.H2('Comments/Minute', style={'paddingTop': '.3rem','fontWeight': 'bold','font-size':'150%','text-align':'center'}),
                        ])
                    ], className="three columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#fdf3cc"}),
                    dbc.Col([
                        html.Div(children=[
                            html.H3(id='Engagement_Num_User', style={'sixWeight': 'bold','color':'blue','font-size':'320%','text-align':'center'}),
                            html.H2('Chat Contributors', style={'paddingTop': '.3rem','fontWeight': 'bold','font-size':'150%','text-align':'center'}),
                        ])
                    ], className="three columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#ccfddb"})
                ] ),

                dbc.Row([
                     dbc.Col([
                        dcc.Tabs([
                            dcc.Tab(label='Timeline', children=[
                                dcc.Graph(id='Engagement_timeline',figure=fig),
                            ],style=tab_sub_style, selected_style=tab_sub_selected_style)
                        ],style=tab_sub_style)
                    ]),
                     dbc.Col([
                        dcc.Tabs([
                            dcc.Tab(label='Top Contributors', children=[
                                #dbc.Col([dcc.Graph(id='Engagement_topuser',figure=fig)])
                                dcc.Graph(id='Engagement_topuser',figure=fig)
                            ],style=tab_sub_style, selected_style=tab_sub_selected_style),
                            dcc.Tab(label='Top Engagement', children=[
                                dcc.Graph(id='Engagement_CountByTime',figure=fig)
                            ],style=tab_sub_style, selected_style=tab_sub_selected_style),
                        ],style=tabs_styles),
                     ])
                ],className="twleve column", style={'padding':'.3rem', 'marginTop':'1rem', 'marginRight':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '20px', 'backgroundColor': 'white', }),
            ], style=tab_style, selected_style=tab_selected_style,),
            #style={'display': 'inline-block', 'width': '49%'},),
        dcc.Tab(label='Sentiment', children=[
                #dbc.Row([ html.Label('Total accidents', style={'paddingTop': '.3rem'}),]),
                dbc.Row([
                    #html.Div(children=[
                        #dbc.Col([dcc.Graph(id='example-graph',figure=fig) ]),dbc.Col([dcc.Graph(id='example-graph2',figure=fig)])
                        dbc.Col([dcc.Graph(id='Sentiment_Polarity_Pie_01',figure=fig) ]), 
                        dbc.Col([
                            dbc.Row([
                                dbc.Col([html.Div(children=[html.H3(id='Sentiment_Positive_Message', style={'fontWeight': 'bold','color':'blue','font-size':'250%','text-align':'center'}),
                                                            html.Label('Positive Message', ),])],className="two columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#ccfddb"}),
                                dbc.Col([html.Div(children=[html.H3(id='Sentiment_Negative_Message', style={'fontWeight': 'bold','color':'blue','font-size':'250%','text-align':'center'}),
                                                            html.Label('Nagative Message', ),])],className="two columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#fdccee"}),
                                dbc.Col([html.Div(children=[html.H3(id='Sentiment_Neutral_Message', style={'fontWeight': 'bold','color':'blue','font-size':'250%','text-align':'center'}),
                                                            html.Label('Neutral Message',   ),])],className="two columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#fdf3cc"}),
                            ]),
                            dbc.Row([
                                dbc.Col([html.Div(children=[html.H3(id='Sentiment_Positive_Average', style={'fontWeight': 'bold','color':'blue','font-size':'250%','text-align':'center'}),
                                                            html.Label('Positive Average', ),])],className="two columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#ccfddb"}),
                                dbc.Col([html.Div(children=[html.H3(id='Sentiment_Negative_Average', style={'fontWeight': 'bold','color':'blue','font-size':'250%','text-align':'center'}),
                                                            html.Label('Nagative Average', ),])],className="two columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#fdccee"}),
                                dbc.Col([html.Div(children=[html.H3(id='Sentiment_Neutral_Average', style={'fontWeight': 'bold','color':'blue','font-size':'250%','text-align':'center'}),
                                                            html.Label('Neutral Average',   ),])],className="two columns",style={'padding':'2rem', 'margin':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '10px', 'marginTop': '2rem',"background-color": "#fdf3cc"}),
                            ])
                        ])
                    
                ],className="twleve column", style={'padding':'.3rem', 'marginTop':'1rem', 'marginRight':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '20px', 'backgroundColor': 'white', }),
                dbc.Row([
                    dbc.Col([dcc.Graph(id='example-graph2',figure=fig)])
                ],className="twleve column", style={'padding':'.3rem', 'marginTop':'1rem', 'marginRight':'1rem', 'boxShadow': '#e3e3e3 4px 4px 2px', 'border-radius': '20px', 'backgroundColor': 'white', }),
            ], style=tab_style, selected_style=tab_selected_style,),

        ],style=tabs_styles),
    ]),

    html.Div([
            #dcc.Graph(id='example-graph2',figure=fig),

        ],style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'})

    ], style={
       'padding': '10px 5px'
    }),
    
])


@app.callback(
    Output('Engagement_CountByTime', 'figure'),
    Input('crossfilter-xaxis-column', 'value'))
def update_graph(crossfilter_xaxis_column):
    print(f"3--> {crossfilter_xaxis_column}")
    ret = df_list.loc[df_list['title'] == crossfilter_xaxis_column,['ch_name', 'vid']]
    #print(ret.loc[:,'ch_name'][0])
    #print(get_sentiment_by_vid(ret['vid']))
    ch_name = ret.loc[:,'ch_name'][0]
    vid = ret.loc[:,'vid'][0]
    _group_channel=f"Engegement_group_by_time_count_name_{ch_name}"
    livechat_collection = livechat_mart_db[_group_channel]
    df=pd.DataFrame()
    c=0
    for i in livechat_collection.find({"vid":vid}):
        data = [[i['aname'],i['time'],i['count']]]
        if c > 0:
            df2 = pd.DataFrame(data, columns=['aname','time','count'])
            #print("append")
            df = pd.concat([df, df2], axis=0)
        else:
            df = pd.DataFrame(data, columns=['aname','time','count'])
        
        c += 1

    title_out = f"{ch_name} : {crossfilter_xaxis_column}"
    fig = px.scatter(df,x="time",y='count' ,color="aname")
    title_out = f"{ch_name} : {crossfilter_xaxis_column}"
    fig.update_layout(title_text=title_out,
                  title_font_size=15,xaxis_title="Time (hour:mintue)")

    return(fig)

@app.callback(
    Output('Engagement_timeline', 'figure'),
    Output('Engagement_ave_min', 'children'),
    Input('crossfilter-xaxis-column', 'value'))
def update_graph(crossfilter_xaxis_column):
    print(f"3--> {crossfilter_xaxis_column}")
    ret = df_list.loc[df_list['title'] == crossfilter_xaxis_column,['ch_name', 'vid']]
    #print(ret.loc[:,'ch_name'][0])
    #print(get_sentiment_by_vid(ret['vid']))
    ch_name = ret.loc[:,'ch_name'][0]
    vid = ret.loc[:,'vid'][0]
    _group_channel=f"Engagement_bytime_{ch_name}"
    livechat_collection = livechat_mart_db[_group_channel]
    df=pd.DataFrame()
    c=0
    for i in livechat_collection.find({"vid":vid}):
        data = [[i['time'],i['count']]]
        if c > 0:
            df2 = pd.DataFrame(data, columns=['time','count'])
            #print("append")
            df = pd.concat([df, df2], axis=0)
        else:
            df = pd.DataFrame(data, columns=['time','count'])
        
        c += 1
    #pos_mean = float("%.2f" %(pos_mean))
    title_out = f"{ch_name} : {crossfilter_xaxis_column}"
    fig = px.line(df,x="time",y='count' )
    fig.update_layout(title_text=title_out,
                  title_font_size=15,xaxis_title="Time (hour:mintue)")

    return(fig, float("%.2f" %(df['count'].mean())) )

@app.callback(
    Output('Engagement_topuser', 'figure'), 
    Output('Engagement_Num_User', 'children'), 
    Output('Engagement_TotalReplay', 'children'), 
    Input('crossfilter-xaxis-column', 'value'))
def update_graph(crossfilter_xaxis_column):
    print(f"3--> {crossfilter_xaxis_column}")
    ret = df_list.loc[df_list['title'] == crossfilter_xaxis_column,['ch_name', 'vid']]
    #print(ret.loc[:,'ch_name'][0])
    #print(get_sentiment_by_vid(ret['vid']))
    ch_name = ret.loc[:,'ch_name'][0]
    vid = ret.loc[:,'vid'][0]
    _group_channel=f"Sentiment_{ch_name}"
    livechat_collection = livechat_mart_db[_group_channel]
    df=pd.DataFrame()
    c=0
    for i in livechat_collection.aggregate([{'$match':{'vid':vid}},{'$group': {'_id': '$aname','count':{'$sum':1}}}, { '$sort' : {'count':-1 }} ]):
        data = [[i['_id'],i['count']]]
        if c > 0:
            df2 = pd.DataFrame(data, columns=['aname','count'])
            #print("append")
            df = pd.concat([df, df2], axis=0)
        else:
            df = pd.DataFrame(data, columns=['aname','count'])
    
        c += 1
    
    df.reset_index(drop=True)
    title_out = f"{ch_name} : {crossfilter_xaxis_column}"
    fig = px.bar(df.iloc[0:10,:], x="count", y="aname", orientation='h')
    fig.update_layout(barmode='stack',yaxis=dict(autorange="reversed"))
    fig.update_layout(title_text=title_out,
                  title_font_size=15,yaxis_title="Contributors")

    return(fig,df['aname'].count(),df['count'].sum())
    
'''
@app.callback(
    Output('example-graph', 'figure'), 
    Input('crossfilter-xaxis-column', 'value'))
def update_graph(crossfilter_xaxis_column):
    #print(f"--> {crossfilter_xaxis_column}")
    ret = df_list.loc[df_list['title'] == crossfilter_xaxis_column,['ch_name', 'vid']]
    #print(get_sentiment_by_vid(ret['vid']))
    data = dd[ret.loc[:,'vid'][0]]
    df = pd.DataFrame({
    "Sentimet": "Value",
    "Score": [data['Neg'], data['Neu'], data['Pos'],data['Compound']],
    "Polarity": ["Negative", "Nue", "Positive", "Compound"]})
    #print(data)
    fig_ret = px.bar(df, x="Sentimet", y="Score", color="Polarity", barmode="group",title=crossfilter_xaxis_column,text_auto='.2s')
    fig.update_traces(textfont_size=18, textangle=0, textposition="inside", cliponaxis=False)
    #fig_ret.update_traces(customdata=dd[ret.loc[:,'vid'][0]])

    return(fig_ret)
'''

@app.callback(
    Output('Sentiment_Polarity_Pie_01', 'figure'),
    Output('Sentiment_Positive_Message', 'children'),
    Output('Sentiment_Negative_Message', 'children'),
    Output('Sentiment_Neutral_Message', 'children'),
    Output('Sentiment_Positive_Average', 'children'),
    Output('Sentiment_Negative_Average', 'children'),
    Output('Sentiment_Neutral_Average', 'children'),
    Input('crossfilter-xaxis-column', 'value'))
def update_graph(crossfilter_xaxis_column):
    print(f"--> 77{crossfilter_xaxis_column}")
    ret = df_list.loc[df_list['title'] == crossfilter_xaxis_column,['ch_name', 'vid']]
    ch_name = ret.loc[:,'ch_name'][0]
    vid = ret.loc[:,'vid'][0]

    data = dd[ret.loc[:,'vid'][0]]

    _group_channel=f"Sentiment_Score_{ch_name}"
    print(f"--> 771{ch_name}  {vid} {_group_channel}")
    livechat_cscore = livechat_mart_db[_group_channel]
    df_sentiment=pd.DataFrame()

    print(f"--> 772{ch_name}  {vid}")
    c=0
    data_out=[]
    for i in livechat_cscore.find({"vid":vid}):
        data = [[i['name'],i['datetime'],i['score'],i['Polarity']]]
        data_out.append(data)

    
    c=0
    for data in data_out:
        if c > 0:
            df_sentiment2 = pd.DataFrame(data, columns=['name', 'datetime','score','Polarity'])
            #print("append")
            df_sentiment = pd.concat([df_sentiment, df_sentiment2], axis=0)
        else:
            df_sentiment = pd.DataFrame(data, columns=['name', 'datetime','score','Polarity'])
        
        c += 1
    
    pos_count = df_sentiment.loc[df_sentiment['Polarity'] == 'POS','name'].count()
    neg_count = df_sentiment.loc[df_sentiment['Polarity'] == 'NEG','name'].count()
    neu_count = df_sentiment.loc[df_sentiment['Polarity'] == 'NEU','name'].count()

    pos_mean = df_sentiment.loc[df_sentiment['Polarity'] == 'POS','score'].mean()
    neg_mean = df_sentiment.loc[df_sentiment['Polarity'] == 'NEG','score'].mean()
    neu_mean = df_sentiment.loc[df_sentiment['Polarity'] == 'NEU','score'].mean()
    
    pos_mean = float("%.2f" %(pos_mean))
    neg_mean = float("%.2f" %(neg_mean))
    neu_mean = float("%.2f" %(neu_mean))

    print(f"count : {pos_count} {neg_count} {neu_count}")
    print(f"mean : {pos_mean} {neg_mean} {neu_mean}")

    fig_ret = px.pie(df_sentiment.loc[df_sentiment['Polarity'] != "NEU"], values='score',names='Polarity',color='Polarity' ,hole=.3,
                    color_discrete_map= {'POS':'rgb(204,253,219)', 'NEG': 'rgb(253,204,238)'})


    title_out = f"{ch_name} : {crossfilter_xaxis_column}"
    fig_ret.update_layout(title_text=title_out,
                  title_font_size=15,yaxis_title="Contributors")
    fig_ret.update_traces(hoverinfo='percent', textfont_size=20,
                  marker=dict(line=dict(color='#000000', width=2)))
    return(fig_ret,pos_count,neg_count,neu_count,pos_mean,neg_mean,neu_mean)


@app.callback(
    Output('example-graph2', 'figure'),
    Input('crossfilter-xaxis-column', 'value'))
def update_graph(crossfilter_xaxis_column):
    print(f"2--> {crossfilter_xaxis_column}")
    ret = df_list.loc[df_list['title'] == crossfilter_xaxis_column,['ch_name', 'vid']]
    #print(ret.loc[:,'ch_name'][0])
    #print(get_sentiment_by_vid(ret['vid']))
    ch_name = ret.loc[:,'ch_name'][0]
    vid = ret.loc[:,'vid'][0]
    print(f"Sentiment_{ch_name}, {vid}")
    mart_db = client_sm['livechat_mart']

    #mart_collection = mart_db[f"Sentiment_{ch_name}"]
    #ret = mart_collection.find({"vid":vid})

    sentiment_group_channel=f"Sentiment_bytime_{ch_name}"
    livechat_collection = livechat_mart_db[sentiment_group_channel]
    df=pd.DataFrame()
    c=0
    for i in livechat_collection.find({"vid":vid}):
        data = [[i['neg'],i['neu'],i['pos'],i['compound'],i['datetime']]]
        if c > 0:
            df2 = pd.DataFrame(data, columns=['neg', 'neu','pos','compound','time'])
            #print("append")
            df = pd.concat([df, df2], axis=0)
        else:
            df = pd.DataFrame(data, columns=['neg', 'neu','pos','compound','time'])
        
        c += 1

    #fig = px.line(df,x="time",y=['neg', 'neu','pos','compound'] )
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df['time'], y=df['neg'], mode='lines',name='Negative',line_color='rgb(230, 70, 64)'))
    fig.add_trace(go.Scatter(x=df['time'], y=df['pos'], mode='lines',name='Positive', line_color='rgb(83, 194, 146)'))
    fig.add_trace(go.Scatter(x=df['time'], y=df['neu'], mode='lines',name='Neutral', line_color='rgb(96, 92, 184)'))
    fig.add_trace(go.Scatter(x=df['time'], y=df['compound'], mode='lines',name='Compound', line_color='rgb(251, 150, 73)'))
    title_out = f"{ch_name} : {crossfilter_xaxis_column}"
    fig.update_layout(title=title_out,
                   xaxis_title='Time (hour:mintue)',
                   yaxis_title='Sum Score')

    return(fig)


if __name__ == '__main__':
    app.run_server(debug=True,host='0.0.0.0',port=8070)
    ii = df.groupby(pd.Grouper(key='time', axis=0, freq='5min'))
    result = ii[['neg','pos','neu','compound']].sum()
    
    
