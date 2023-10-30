# importing the packages
import datetime
from numpy import datetime64
import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import urllib
import ssl
import sqlite3

def Get_data():
    # create mongodb connection
    username = "sankarallof" ## put your userid
    password = "Sankar2002" ### ur pass word

    # Encode the username and password
    encoded_username = urllib.parse.quote_plus(username)
    encoded_password = urllib.parse.quote_plus(password)

    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Construct the URI with encoded credentials
    uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.nb81bns.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)

    # create database name and collection name
    db = client['Youtube']
    collection = db['channels']
    
    # create project title name
    st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    with st.sidebar:
        channel_name = st.text_input("Enter Channel Name")
        get_data = st.button("Get Data")
    if get_data:
    # create api connection
        def API_connect():
            API_key = "AIzaSyCD3UM3pwY1k-jE_hTmFqoGPnG_gwDeIDk"
            api_service_name = "youtube"
            api_version = "v3"
            youtube = build(api_service_name,api_version, developerKey=API_key)
            return youtube
        youtube = API_connect()

        # get channel id
        
        request = youtube.search().list(
            part = "id,snippet",
            channelType = "any",
            maxResults = 1,
            q = channel_name
        )
        response = request.execute()

        channel_id = response['items'][0]['id']['channelId']

        # get channel details
        request = youtube.channels().list(
            part = "snippet,contentDetails,statistics",
            id = channel_id)
        response = request.execute()

        channelDetails = dict(channelId = response['items'][0]['id'],
                            
                            channelName = response['items'][0]['snippet']['title'],
                            channelDescription = response['items'][0]['snippet']['description'],
                            subscriberCount = response['items'][0]['statistics']['subscriberCount'],
                            viewCount = response['items'][0]['statistics']['viewCount'],
                            videoCount = response['items'][0]['statistics']['videoCount'],
                            uploadId = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                            publishDate = response['items'][0]['snippet']['publishedAt'])
        uploadId = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        channelDetails_list = []
        channelDetails_list.append(channelDetails)

        # create channel dataframe
        #channel_df = pd.DataFrame(channelDetails_list)
        

        # get video id's
        playlist_id = uploadId

        def get_video_id(youtube,playlist_id):
            request = youtube.playlistItems().list(
                part = "contentDetails",
                playlistId = playlist_id,
                maxResults = 50
            )
            response = request.execute()

            Video_id = []
            for item in response['items']:
                video_id = item['contentDetails']['videoId']
                Video_id.append(video_id)

            next_page_token = response.get('nextPageToken')
            more_page = True

            while more_page:
                if next_page_token is None:
                    more_page = False
                else:
                    request = youtube.playlistItems().list(
                        part = "snippet,contentDetails",
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
                    response = request.execute()

                    for item in response['items']:
                        video_id = item['contentDetails']['videoId']
                        Video_id.append(video_id)

                    next_page_token = response.get('nextPageToken')

            return Video_id
        
        ids1 = get_video_id(youtube,playlist_id)
        
        # get video details
        def get_video_details(youtube,ids1):
            video_detais = []
            for i in ids1:
                request = youtube.videos().list(
                    part = "snippet,statistics",
                    id = i)

                response = request.execute()


                for video in response['items']:
                    videos = dict(
                        ChannelId = video['snippet']['channelId'],
                        Video_Id = i,
                        Video_title = video['snippet']['title'],
                        Video_Description = video['snippet']['description'],
                        Video_PublishDate = video['snippet']['publishedAt'],
                        Video_ViewCount = video['statistics']['viewCount'],
                        Video_LikeCount = video['statistics']['likeCount'],
                        Video_CommentCount = video['statistics']['commentCount']
                    )
                    video_detais.append(videos)

            return video_detais
        
        cv = get_video_details(youtube,ids1)

        # create video dataframe
        #video_df = pd.DataFrame(cv)
        #video_df['Video_CommentCount'] = pd.to_numeric(video_df['Video_CommentCount'])
        #video_df['Video_LikeCount'] = pd.to_numeric(video_df['Video_LikeCount'])
        #video_df['Video_ViewCount'] = pd.to_numeric(video_df['Video_ViewCount'])
        #video_df['Video_PublishDate'] = pd.to_datetime(video_df['Video_PublishDate'], format="%Y-%m-%d %H:%M:%S")

        # get comment details
        def get_comment_details(id):
            comments = []
            next_page_token = None

            while True:
                for i in id:
                    request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=i,
                        textFormat="plainText",
                        pageToken=next_page_token,
                        maxResults = 3
                    )
                    response = request.execute()

                    for item in response["items"]:
                        comment_detail = dict(commentId = item["snippet"]["topLevelComment"]["id"],
                        videoId = item["snippet"]["videoId"],
                        commentAuthorName = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        commentText = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        commentPublishDate = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                        commentLikeCount = item["snippet"]["topLevelComment"]["snippet"]["likeCount"],
                        commentReplyCount = item["snippet"]["totalReplyCount"])
                        comments.append(comment_detail)

                next_page_token = response.get("nextPageToken")

                if not next_page_token:
                    break

            return comments
        
        com = get_comment_details(ids1)

        # create comment dataframe
        #comment_df = pd.DataFrame(com)
        #comment_df['commentPublishDate'] = pd.to_datetime(comment_df['commentPublishDate'], format="%Y-%m-%d %H:%M:%S")

        # create json for save mongodb
        channel_Information = dict(ChannelDetails=channelDetails,VideoDetails=cv,CommentDetails=com)

        if collection.find_one({'ChannelDetails.channelName' : channel_Information['ChannelDetails']['channelName']}):
            
                st.warning("This channel name is already existed")
                st.warning("Please try another channel name")
        else:
            
            st.json(channel_Information)
            collection.insert_one(channel_Information)
            st.success("This channel information is successfully inserted")


def clean_process():

    

    # create mongodb connection
    username = "sankarallof" ## put your userid
    password = "Sankar2002" ### ur pass word

    # Encode the username and password
    encoded_username = urllib.parse.quote_plus(username)
    encoded_password = urllib.parse.quote_plus(password)

    ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Construct the URI with encoded credentials
    uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.nb81bns.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)

    # create database name and collection name
    db = client['Youtube']
    collection = db['channels']

    # create dropdown for channel name list
    channelList = []
    for i in collection.find({},{'ChannelDetails.channelName':1, '_id':0}):
        channelList.append(i['ChannelDetails']['channelName'])
    # create project title name
    st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    with st.sidebar:
        option = st.selectbox("Select channel name",channelList,index=None,placeholder='Selct channel name')
        migrate = st.button("Migrate")
    channel = []
    for i in collection.find({'ChannelDetails.channelName':option},{'ChannelDetails':1, '_id':0}):
        channel.append(i['ChannelDetails'])
    video = []
    
    for i in collection.find({'ChannelDetails.channelName':option},{'VideoDetails':1, '_id':0}):
        video.append(i['VideoDetails'])
        print(video)
    comment = []
    for i in collection.find({'ChannelDetails.channelName':option},{'CommentDetails':1, '_id':0}):
        comment.append(i['CommentDetails'])
    channel_df = pd.DataFrame(channel)

    video_df = pd.DataFrame(video,index=[0])
    commment_df = pd.DataFrame(comment)

    if option:
        
        st.write("Channel Detail Dataframe")
        st.dataframe(channel_df)
        st.write("Video Detail Dataframe")
        st.dataframe(video_df)
        st.write("Comment Detail Dataframe")
        st.dataframe(commment_df)
    # Insert data to sql

    if migrate:
        # create sql connection
        conn = sqlite3.connect("youtube.db")
        cursor = conn.cursor()

        # create tables
        channel_table = ''' CREATE TABLE IF NOT EXISTS channel (
            
            channelId TEXT ,
            channelName TEXT PRIMARY KEY,
            channelDescription TEXT,
            subscriberCount INTEGER,
            viewCount INTEGER,
            videoCount INTEGER,
            uploadId TEXT,
            publishDate DATETIME
        )'''

        video_table = ''' CREATE TABLE IF NOT EXISTS video (
            
            ChannelId TEXT,
            Video_Id TEXT PRIMARY KEY,
            Video_title TEXT,
            Video_Description TEXT,
            Video_PublishDate DATETIME,
            Video_ViewCount INTEGER,
            Video_LikeCount INTEGER,
            Video_CommentCount INTEGER

        )'''

        comment_table = ''' CREATE TABLE IF NOT EXISTS comment (
            
            commentID TEXT PRIMARY KEY,
            videoId TEXT,
            commentAuthorName TEXT,
            commentText TEXT,
            commentPulishDate DATETIME,
            commentLikeCount INTEGER,
            commentReplyCount INTEGER
        )'''

        cursor.execute(channel_table)
        cursor.execute(video_table)
        cursor.execute(comment_table)


        channel_df['subscriberCount'] = channel_df['subscriberCount'].astype(int)
        channel_df['viewCount'] = channel_df['viewCount'].astype(int)
        channel_df['videoCount'] = channel_df['videoCount'].astype(int)
        channel_df['publishDate'] = pd.to_datetime(channel_df['publishDate'])
        channel_df['PublishDate'] = channel_df['publishDate'].dt.date
        channel_df['PublishDate'] = pd.to_datetime(channel_df['PublishDate'])
        
        st.dataframe(channel_df['PublishDate'])
        st.write(channel_df['publishDate'].dtypes)
        video_df['Video_ViewCount'] = video_df['Video_ViewCount'].astype(int)
        video_df['Video_LikeCount'] = video_df['Video_LikeCount'].astype(int)
        video_df['Video_CommentCount'] = video_df['Video_CommentCount'].astype(int)
        video_df['Video_PublishDate'] = pd.to_datetime(video_df['Video_PublishDate'])
        video_df['Video_publishDate'] = video_df['Video_PublishDate'].dt.date
        video_df['Video_PublishDate'] = pd.to_datetime(video_df['Video_PublishDate'])

        commment_df['commentPulishDate'] = pd.to_datetime(commment_df['commentPulishDate'])
        commment_df['commentPulish_Date'] = commment_df['commentPulishDate'].dt.date
        commment_df['commentPulish_Date'] = pd.to_datetime(commment_df['commentPulish_Date'])

        channel_sql = "SELECT * FROM channel"
        channel_sql_df = pd.read_sql_query(channel_sql,conn)
        st.dataframe(channel_sql_df)
        
        #checkName = list(channel_sql_df['channelName']).index(option)
        
        #if option == channel_sql_df['channelName'][checkName] :
            #with st.sidebar:
                #st.warning("This channel name already existed")
        #else:
        for row in channel_df.itertuples():
            cursor.execute(f"INSERT INTO channel (channelId,channelName,channelDescription,subscriberCount,viewCount,videoCount,uploadId,publishDate) VALUES ('{row[1]}','{row[2]}','{row[3]}',{row[4]},{row[5]},{row[6]},'{row[7]}','{row[9]}')")

        for row in video_df.itertuples():
            cursor.execute(f"INSERT INTO video (channelId,Video_Id,Video_title,Video_Description,Video_PublishDate,Video_ViewCount,Video_LikeCount,Video_CommentCount) VALUES ('{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[9]}',{row[6]},{row[7]},{row[8]})")

        for row in commment_df.itertuples():
            cursor.execute(f"INSERT INTO comment (commentID,videoId,commentAuthorName,commentText,commentPulishDate,commentLikeCount,commentReplyCount) VALUES ('{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[8]}',{row[6]},{row[7]})")

            
        with st.sidebar:
            st.success("Data Successfully Inserted")
            
        conn.commit()
        conn.close()





with st.sidebar:
    func = st.selectbox("Select the Function name",['Get_data','clean_process','queries'],index=None,placeholder="Select the Function name")

if "Get_data" == func:
    Get_data()

if "clean_process" == func:
    clean_process()




        

        


  




    
