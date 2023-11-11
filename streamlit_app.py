import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from PIL import Image
import boto3
import json

st.set_page_config(layout="wide")

dynamo_client  =  boto3.resource(service_name = "dynamodb",region_name = "us-east-1",
               aws_access_key_id = "AKIA2VPJSHXG7T5PQ5WM",
               aws_secret_access_key = "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")

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



image = Image.open('BITS_Pilani_Logo.png')


col1, col2, col3,col4, col5 = st.columns([1,2,4,2,1],gap="large")
with col1:
   st.image(image)
with col3:
    st.title("Real Time Tweet Analysis ")

col1,col2,col3 = st.columns([1,4,1])
with col2:
    user_selected = st.selectbox("Which Tweeter Handle you want to check?",df_tweetData['UserName'].unique())    

df_tweetData_filtered = df_tweetData[df_tweetData['UserName']==str(user_selected)]

col1,col2,col3,col4,col5,col6,col7,col8,col9 = st.columns(9)
with col2:
    st.metric(label="Total Tweets", value=str(len(df_tweetData_filtered.index)))
with col4:
    st.metric(label="Total Replies", value=str(len(df_tweetReply.index)))
with col6:
    st.metric(label="Total Non English Tweets", value=str(len(df_tweetData_filtered[df_tweetData_filtered['language'] == 'en'].index)))
with col8:
    st.metric(label="Total Sentiments Analyzed", value=str(len(df_tweetReply[df_tweetReply['sentiment'] != 'TBD'].index))) 


st.write(user_selected)
st.write(df_tweetData)

st.write(df_tweetReply)