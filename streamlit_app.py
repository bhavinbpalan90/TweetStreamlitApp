import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from PIL import Image
import boto3
import json
from st_aggrid import AgGrid
import requests
import time


st.set_page_config(layout="wide")


from botocore.exceptions import ClientError

def printTweets(df_tweetData_filtered_selected,mergedTweet_filtered):
    df_tweetData_filtered_selected = df_tweetData_filtered_selected.reset_index()
    totallength = len(df_tweetData_filtered_selected.index) - 1
    choice = st.selectbox("Select Tweet:",df_tweetData_filtered_selected['Tweet'])
    tempvalue = df_tweetData_filtered_selected.index[df_tweetData_filtered_selected['Tweet']==choice].tolist()
    selectedvalue = tempvalue[0]
    st.markdown('-------------------------------------------------------------------------')
    st.write('**Tweet:** ' + str(df_tweetData_filtered_selected.iloc[selectedvalue]['Tweet']))
    col1,col2 = st.columns(2)
    with col1:
        st.write('**Replies:** ' + str(df_tweetData_filtered_selected.iloc[selectedvalue]['Replies Count']))
        st.write('**Retweets:** ' + str(df_tweetData_filtered_selected.iloc[selectedvalue]['Retweet Count']))
        st.write('**Views:** ' + str(df_tweetData_filtered_selected.iloc[selectedvalue]['Views Count']))
    with col2:
        st.write('**Posted On:** ' + str(df_tweetData_filtered_selected.iloc[selectedvalue]['Posted On']))
    st.write('**Translated in English:** ' + str(df_tweetData_filtered_selected.iloc[selectedvalue]['Tweet in English']))
    st.divider()
    st.write('**Below are the last 20 replies on this tweet:** ')
    mergedTweet_filtered_selected = mergedTweet_filtered[mergedTweet_filtered['tweet_id_x']==df_tweetData_filtered_selected.iloc[selectedvalue]['tweet_id']]
    mergedTweet_filtered_selected = mergedTweet_filtered_selected.rename(columns={"text_y":"Original Reply","engText":"Translated to English","sentiment":"Sentiment Analyzed","creation_date_y":"Posted On"})
    AgGrid(mergedTweet_filtered_selected[["Posted On","Original Reply","Sentiment Analyzed","Translated to English"]].sort_values(by=['Posted On'], ascending=False).head(20)
       , fit_columns_on_grid_load = True
       ,width = '100%'
       )
    return 'Completed'

def get_secrets():
    secret_name = "DynamoDB"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name = 'secretsmanager', region_name = region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    return secret




access_id = get_secrets()["aws_access_key_id"]
access_key = get_secrets()["aws_secret_access_key"]

dynamo_client  =  boto3.resource(service_name = "dynamodb",region_name = "us-east-1",
               aws_access_key_id = access_id,
               aws_secret_access_key = access_key)

dynamo_client.get_available_subresources()

## Below code is to get all TweetData
TweetTable = dynamo_client.Table("TweetData")
responseTweet = TweetTable.scan()
dataTweet = responseTweet['Items']
df_tweetData = pd.DataFrame(dataTweet)

## Below code is to get all TweetReplies Data
TweetReplyTable = dynamo_client.Table("TweetReplies")
responseTweetReply = TweetReplyTable.scan()
dataTweetReply = responseTweetReply['Items']
df_tweetReply = pd.DataFrame(dataTweetReply)

mergedTweet = pd.merge(left=df_tweetData, right=df_tweetReply, left_on='tweet_id', right_on='in_reply_to_status_id')

def get_tweets(userName):
    url = "https://twitter154.p.rapidapi.com/user/tweets"
    querystring = {"username":userName,"limit":"100","include_replies":"true","include_pinned":"false"}

    headers = {
        "X-RapidAPI-Key": "9dcf7696a3msh7ca39b3615b7a51p1a2e37jsn2153fbdeca33",
        "X-RapidAPI-Host": "twitter154.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    df = pd.DataFrame(response.json()['results'])
    df = df.reset_index()
    for index, row in df.iterrows():
        get_tweetReplies(row['tweet_id'])
    df['UserName'] = str(userName)
    ##print('------------------------')
    ##print(list(df.columns))
    final_df = df[['tweet_id','creation_date','text','language','favorite_count','retweet_count','reply_count','views','UserName','expanded_url']]
    final_df = final_df.to_dict(orient = 'records')
    for i in final_df:
        TweetTable.put_item(Item = i)
        time.sleep(2)
    ##print("Completed")    

def get_tweetReplies(tweetID):
    url = "https://twitter154.p.rapidapi.com/tweet/replies"
    querystring = {"tweet_id":tweetID}

    headers = {
        "X-RapidAPI-Key": "9dcf7696a3msh7ca39b3615b7a51p1a2e37jsn2153fbdeca33",
        "X-RapidAPI-Host": "twitter154.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    if response.json()['replies']:
        df = pd.DataFrame(response.json()['replies'])
        ##print('------------------------')
        ##print(list(df.columns))
        df['sentiment'] = 'TBD'
        final_df = df[['tweet_id','creation_date','text','in_reply_to_status_id','language','sentiment']]
        final_df = final_df.to_dict(orient = 'records')
        for i in final_df:
            TweetReplyTable.put_item(Item = i)
            time.sleep(2)
    #print(response.json())


image = Image.open('BITS_Pilani_Logo.png')


col1, col2, col3,col4, col5 = st.columns([1,2,4,2,1],gap="large")
with col1:
   st.image(image)
with col3:
    st.title("Real Time Tweet Analysis ")

with st.sidebar:
    userName = st.text_input('Enter Twitter User Handle', 'narendramodi')
    if st.button('Get Tweet Data'):
        get_tweets(userName)

col1,col2,col3,col4,col5,col6,col7,col8,col9 = st.columns(9)
with col2:
    st.metric(label="Total Tweets", value=str(len(df_tweetData.index)))
with col4:
    st.metric(label="Total Replies", value=str(len(df_tweetReply.index)))
with col6:
    st.metric(label="Total Non English Tweets", value=str(len(df_tweetData[df_tweetData['language'] == 'en'].index)))
with col8:
    st.metric(label="Total Sentiments Analyzed", value=str(len(df_tweetReply[df_tweetReply['sentiment'] != 'TBD'].index)))


st.divider()

col1,col2,col3 = st.columns([1,4,1])
with col2:
    user_selected = st.selectbox("Which Tweeter Handle you want to check?",df_tweetData['UserName'].unique())

df_tweetData_filtered = df_tweetData[df_tweetData['UserName']==str(user_selected)]
mergedTweet_filtered = mergedTweet[mergedTweet['UserName']==str(user_selected)]


df_tweetData_filtered = df_tweetData_filtered.rename(columns={"text" : "Tweet","reply_count":"Replies Count","retweet_count":"Retweet Count","views":"Views Count","creation_date":"Posted On",
                                      "textEng":"Tweet in English"})

df_tweetData_filtered_selected = df_tweetData_filtered[["Tweet","Replies Count","Retweet Count","Views Count","Posted On","Tweet in English","tweet_id"]]

df_tweetData_filtered_selected = df_tweetData_filtered_selected.sort_values(by=['Posted On'], ascending=False)

printTweets(df_tweetData_filtered_selected,mergedTweet_filtered)

st.divider()

st.markdown(''' :blue[Services Used by Group 3:   ] 
    :red[Kinesis Data Stream], :orange[Lambda], :green[Secret Manager], :blue[DynamoDB], :violet[Comprehend],
    :gray[Translate], :rainbow[EC2], :red[Route53].''', unsafe_allow_html=False)
