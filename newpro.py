#!/usr/bin/env python
# coding: utf-8

# In[9]:


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import psycopg2
import pandas as pd
import streamlit as st
from datetime import datetime
import re
import json

# MongoDB connection
mongo_client = MongoClient("mongodb+srv://swarna311294:Seswa123@cluster0nag.ihndsxb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0NAG")
db = mongo_client['youtube']

# PostgreSQL connection
connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Seswa@123",
    database="youtube",
    port="5432"
)
mycursor = connection.cursor()

# API connection
def api_connect():
    api_key = "AIzaSyDJMP2ZHAEUMIw14-OyrpVbfE-b-7hK4FI"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube
    
youtube = api_connect()

# Function to get channel info
def get_channel_info(channel_id):
    try:
        request = youtube.channels().list(part="snippet,ContentDetails,statistics", id=channel_id)
        response = request.execute()
        data = {
            "Channel_Name": response['items'][0]['snippet']['title'],
            "Channel_Id": response['items'][0]['id'],
            "Subscribers": response['items'][0]['statistics']['subscriberCount'],
            "Views": response['items'][0]['statistics']['viewCount'],
            "Total_Videos": response['items'][0]['statistics']['videoCount'],
            "Channel_Description": response['items'][0]['snippet']['description'],
            "Playlist_Id": response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
        return data
    except HttpError as e:
        st.error(f"Error getting channel info: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None


# Function to get video ids dynamically based on channel id
def get_video_ids(channel_id):
    try:
        video_ids = []
        response = youtube.channels().list(
            id=channel_id,
            part='contentDetails'
        ).execute()
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token = None
        while True:
            response = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
            for item in response['items']:
                video_ids.append(item['snippet']['resourceId']['videoId'])
            next_page_token = response.get('nextPageToken')

            if next_page_token is None:
                break

        return video_ids
    except HttpError as e:
        st.error(f"Error getting video ids: {e}")
        return []

# Function to get video info
def get_video_info(channel_id):
    video_data = []
    try:
        video_ids = get_video_ids(channel_id) 
        for video_id in video_ids:
            request = youtube.videos().list(
                part='snippet, contentDetails, statistics',
                id=video_id
            )
            response = request.execute()
            for item in response['items']:
                data = {
                    "Channel_Name": item['snippet']['channelTitle'],
                    "Channel_Id": item['snippet']['channelId'],
                    "Video_Id": item['id'],
                    "Title": item['snippet']['title'],
                    "Tags": item.get('tags'),
                    "Thumbnail": item['snippet']['thumbnails'],
                    "Description": item.get('description'),
                    "Published_Date": item['snippet']['publishedAt'],
                    "Duration": item['contentDetails']['duration'],
                    "Likes": item['statistics'].get('likeCount'),
                    "Views": item['statistics'].get('viewCount'),
                    "Comments": item['statistics'].get('commentCount'),
                    "Favorite_Count": item['statistics']['favoriteCount'],
                    "Definition": item['contentDetails']['definition'],
                    "Caption_Status": item['contentDetails']['caption']
                }
                video_data.append(data)
        return video_data
    except HttpError as e:
        st.error(f"Error getting video info: {e}")
        return []


        
# Function to get comment info
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
            for item in response['items']:
                data = {
                    "Comment_Id": item['snippet']['topLevelComment']['id'],
                    "Video_Id": item['snippet']['topLevelComment']['snippet']['videoId'],
                    "Comment_Text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_Published": item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comment_data.append(data)
        return comment_data
    except HttpError as e:
        st.error(f"Error getting comment info: {e}")
        return []

# Function to get playlist details
def get_playlist_details(channel_id):
    try:
        next_page_token = None
        all_data = []
        while True:
            request = youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            for item in response['items']:
                data = {
                    "Playlist_Id": item['id'],
                    "Title": item['snippet']['title'],
                    "Channel_Id": item['snippet']['channelId'],
                    "Channel_Name": item['snippet']['channelTitle'],
                    "PublishedAt": item['snippet']['publishedAt'],
                    "Video_Count": item['contentDetails']['itemCount']
                }
                all_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
        return all_data
    except HttpError as e:
        st.error(f"Error getting playlist details: {e}")
        return []

# Function to upload to MongoDB
def channel_details(channel_id):
    try:
        ch_details = get_channel_info(channel_id)
        pl_details = get_playlist_details(channel_id)
        vi_ids = get_video_ids(channel_id)
        vi_details = get_video_info(channel_id)
        com_details = get_comment_info(vi_ids)

        col = db["channel_details"]
        col.insert_one({
            "channel_information": ch_details,
            "playlist_information": pl_details,
            "video_information": vi_details,
            "comment_information": com_details
        })

        return "Upload completed successfully"
    except Exception as e:
        return f"Error: {str(e)}"

# Function to check if a table exists
def table_exists(table_name):
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
        )
    """
    mycursor.execute(query, (table_name,))
    return mycursor.fetchone()[0]

# Function to create tables if they don't exist
def create_tables_if_not_exist():
    tables = ["channels", "playlists", "videos", "comments"]
    for table in tables:
        if not table_exists(table):
            create_table_query = ""
            if table == "channels":
                create_table_query = """
                    CREATE TABLE channels (
                        -- Define columns for channels table
                        Channel_Name VARCHAR(255),
                        Channel_Id VARCHAR(255) PRIMARY KEY,
                        Subscribers INTEGER,
                        Views INTEGER,
                        Total_Videos INTEGER,
                        Channel_Description TEXT,
                        Playlist_Id VARCHAR(255)
                    )
                """
            elif table == "playlists":
                create_table_query = """
                    CREATE TABLE playlists (
                        -- Define columns for playlists table
                        Playlist_Id VARCHAR(255) PRIMARY KEY,
                        Title VARCHAR(255),
                        Channel_Id VARCHAR(255),
                        Channel_Name VARCHAR(255),
                        PublishedAt TIMESTAMP,
                        Video_Count INTEGER
                    )
                """
            elif table == "videos":
                create_table_query = """
                    CREATE TABLE videos (
                        -- Define columns for videos table
                        Channel_Name VARCHAR(255),
                        Channel_Id VARCHAR(255),
                        Video_Id VARCHAR(255) PRIMARY KEY,
                        Title VARCHAR(255),
                        Thumbnail TEXT,
                        Description TEXT,
                        Published_Date TIMESTAMP,
                        Duration INTEGER,
                        Views INTEGER,
                        Likes INTEGER,
                        Comments INTEGER,
                        Favorite_Count INTEGER,
                        Definition VARCHAR(50),
                        Caption_Status VARCHAR(50)
                    )
                """
            elif table == "comments":
                create_table_query = """
                    CREATE TABLE comments (
                        -- Define columns for comments table
                        Comment_Id VARCHAR(255),
                        Video_Id VARCHAR(255),
                        Comment_Text TEXT,
                        Comment_Author VARCHAR(255),
                        Comment_Published TIMESTAMP
                    )
                """
            
            # Execute the create table query
            mycursor.execute(create_table_query)
            connection.commit()
            print(f"Table '{table}' created successfully.")

# Call the function to create tables if they don't exist
create_tables_if_not_exist()


# Streamlit Functions
def show_channels_table():
    ch_list = []
    col = db["channel_details"]

    try:
        for ch_data in col.find({}, {"_id": 0, "channel_information": 1}):
            try:
                channel_info = ch_data.get("channel_information")
                if channel_info:
                    data = {
                        "Channel_Name": channel_info.get("Channel_Name", ""),
                        "Channel_Id": channel_info.get("Channel_Id", ""),
                        "Subscribers": channel_info.get("Subscribers", ""),
                        "Views": channel_info.get("Views", ""),
                        "Total_Videos": channel_info.get("Total_Videos", ""),
                        "Channel_Description": channel_info.get("Channel_Description", ""),
                        "Playlist_Id": channel_info.get("Playlist_Id", ""),
                    }
                    ch_list.append(data)
                else:
                    st.warning("Document does not contain 'channel_information'.")
            except KeyError as e:
                st.error(f"Error: {e}")
                return None  # Return None to indicate an error
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None  # Return None to indicate an error

    if ch_list:
        channels_table = pd.DataFrame(ch_list)
        st.table(channels_table)
        return channels_table

    else:
        st.warning("No channel information found.")
        return None  # Return None to indicate an error


def show_playlists_table():
    pl_list = []
    col1 = db["channel_details"]

    for pl_data in col1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data.get("playlist_information", []))):
            pl_list.append(pl_data["playlist_information"][i])

    playlists_table = st.dataframe(pl_list)
    return playlists_table


def show_videos_table():
    vi_list = []
    col2 = db["channel_details"]

    for vi_data in col2.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data.get("video_information", []))):
            vi_list.append(vi_data["video_information"][i])

    videos_table = st.dataframe(vi_list)
    return videos_table


def show_comments_table():
    com_list = []
    col3 = db["channel_details"]

    for com_data in col3.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data.get("comment_information", []))):
            com_list.append(com_data["comment_information"][i])

    comments_table = st.dataframe(com_list)
    return comments_table

# Function to get all channel names from MongoDB
def get_channel_names():
    col = db["channel_details"]
    channels = col.find({}, {"_id": 0, "channel_information.Channel_Name": 1})
    channel_names = [channel["channel_information"]["Channel_Name"] for channel in channels]
    return channel_names



# Function to parse duration string into seconds
def parse_duration(duration_str):
    if not duration_str:
        return None

    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if match:
        hours = int(match.group(1)[:-1]) if match.group(1) else 0
        minutes = int(match.group(2)[:-1]) if match.group(2) else 0
        seconds = int(match.group(3)[:-1]) if match.group(3) else 0
        return hours * 3600 + minutes * 60 + seconds
    else:
        return None

# Function to migrate data of selected channel to PostgreSQL
def migrate_to_postgres(selected_channel):
    try:
        # Fetch data of selected channel from MongoDB
        col = db["channel_details"]
        channel_data = col.find_one({"channel_information.Channel_Name": selected_channel})
        if not channel_data:
            raise ValueError(f"Channel '{selected_channel}' not found in MongoDB.")

        # Insert data into PostgreSQL tables
        if channel_data:
            # Insert channel details into the 'channels' table
            channel_info = channel_data.get("channel_information")
            if channel_info:
                mycursor.execute('''INSERT INTO channels (
                    Channel_Name,
                    Channel_Id,
                    Subscribers,
                    Views,
                    Total_Videos,
                    Channel_Description,
                    Playlist_Id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)''', (
                    channel_info.get('Channel_Name', ''),
                    channel_info.get('Channel_Id', ''),
                    channel_info.get('Subscribers', ''),
                    channel_info.get('Views', ''),
                    channel_info.get('Total_Videos', ''),
                    channel_info.get('Channel_Description', ''),
                    channel_info.get('Playlist_Id', '')
                ))

            # Insert playlist details into the 'playlists' table
            playlist_info = channel_data.get("playlist_information")
            if playlist_info:
                for playlist in playlist_info:
                    mycursor.execute('''INSERT INTO playlists (
                        Playlist_Id,
                        Title,
                        Channel_Id,
                        Channel_Name,
                        PublishedAt,
                        Video_Count
                    ) VALUES (%s, %s, %s, %s, %s, %s)''', (
                        playlist.get('Playlist_Id', ''),
                        playlist.get('Title', ''),
                        playlist.get('Channel_Id', ''),
                        playlist.get('Channel_Name', ''),
                        playlist.get('PublishedAt', ''),
                        playlist.get('Video_Count', '')
                    ))

            # Inserted video details into the 'videos' table
            video_info = channel_data.get("video_information")
            if video_info:
                for video in video_info:
                    # Added the thumbnail_url variable here
                    thumbnail_url = None
                    if isinstance(video['Thumbnail'], dict):
                        thumbnail_url = video['Thumbnail']['default']['url']
                    elif isinstance(video['Thumbnail'], str):
                        thumbnail_url = video['Thumbnail']

                    # Converted published date to string format
                    published_date_str = datetime.strptime(video['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

                    # Parsed duration string into seconds
                    duration_seconds = parse_duration(video['Duration'])
                    if duration_seconds is None:
                        print("Error parsing duration for video:", video['Video_Id'])
                        continue
                                
                    
                    mycursor.execute('''INSERT INTO videos (
                        Channel_Name,
                        Channel_Id,
                        Video_Id,
                        Title,
                        Thumbnail,
                        Description,
                        Published_Date,
                        Duration,
                        Views,
                        Likes,
                        Comments,
                        Favorite_Count,
                        Definition,
                        Caption_Status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (
                        video.get('Channel_Name', ''),
                        video.get('Channel_Id', ''),
                        video.get('Video_Id', ''),
                        video.get('Title', ''),
                        thumbnail_url,  # Using the thumbnail_url variable here
                        video.get('Description', ''),
                        published_date_str,
                        duration_seconds,
                        video.get('Views', ''),
                        video.get('Likes', ''),
                        video.get('Comments', ''),
                        video.get('Favorite_Count', ''),
                        video.get('Definition', ''),
                        video.get('Caption_Status', '')
                    ))

                    # Inserted comment details into the 'comments' table
                    comment_info = video.get("comment_information")
                    if comment_info:
                        for comment in comment_info:
                            mycursor.execute('''INSERT INTO comments (
                                Comment_Id,
                                Video_Id,
                                Comment_Text,
                                Comment_Author,
                                Comment_Published
                            ) VALUES (%s, %s, %s, %s, %s)''', (
                                comment.get('Comment_Id', ''),
                                video.get('Video_Id', ''),
                                comment.get('Comment_Text', ''),
                                comment.get('Comment_Author', ''),
                                comment.get('Comment_Published', '')
                            ))

            connection.commit()
            st.success("Data migrated to PostgreSQL successfully!")

    except Exception as e:
        st.error(f"Error migrating data to PostgreSQL: {str(e)}")


# Streamlit App
# Set Streamlit layout to centered
st.set_page_config(layout="centered")

# Centered heading
# Streamlit UI
st.title("Channel Data Migration")

channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Get and Store data"):
    youtube = api_connect()  
    for channel in channels:
        ch_ids = []
        db = mongo_client["youtube"]
        col = db["channel_details"]
        for ch_data in col.find({}, {"_id": 0, "channel_information": 1}):
            st.write("Current Channel Data:", ch_data)
            try:
                channel_info = ch_data["channel_information"]
                if channel_info and "Channel_Id" in channel_info:
                    ch_ids.append(channel_info["Channel_Id"])
                else:
                    st.error(f"Channel information is missing or does not contain 'Channel_Id' for {channel}")
            except Exception as e:
                st.error(f"An error occurred while processing channel information: {e}")


        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exist")
        else:
            output = channel_details(channel)
            st.success(output)


# Dropdown with all channel names in MongoDB
channel_names = get_channel_names()
selected_channel = st.selectbox("Select Channel:", channel_names)

# SQL Button to migrate data to PostgreSQL
if st.button("Migrate to PostgreSQL"):
    migrate_to_postgres(selected_channel)
if __name__ == "__main__":    
 show_table = st.radio("SELECT THE TABLE FOR VIEW",(":black[channels]",":black[playlists]",":black[videos]",":black[comments]"))

if show_table == ":black[channels]":
    show_channels_table()
elif show_table == ":black[playlists]":
    show_playlists_table()
elif show_table ==":black[videos]":
    show_videos_table()
elif show_table == ":black[comments]":
    show_comments_table()

question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. views of each channel',
     '8. videos published in the year 2023',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))

     
if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    mycursor.execute(query1)
    t1=mycursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    mycursor.execute(query2)
    
    t2=mycursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    mycursor.execute(query3)
    
    t3 = mycursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    mycursor.execute(query4)
    
    t4=mycursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    mycursor.execute(query5)
    
    t5 = mycursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    mycursor.execute(query6)
    
    t6 = mycursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    mycursor.execute(query7)
    
    t7=mycursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2023':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2023;'''
    mycursor.execute(query8)
    
    t8=mycursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    mycursor.execute(query9)
    
    t9=mycursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    mycursor.execute(query10)
    
    t10=mycursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))

connection.close()
mycursor.close()



# In[ ]:





# In[ ]:




