import os
import requests
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import math
import psycopg2
import psycopg2.extras

def parse_video_json(video):

    video_id = video["id"]

    # snippet
    publish_datetime = video["snippet"]["publishedAt"]
    channel_id = video["snippet"]["channelId"]
    video_title = video["snippet"]["title"]
    video_description = video["snippet"]["description"]
    channel_title = video["snippet"]["channelTitle"]
    num_tags = len(video["snippet"].get("tags", [])) # can be null
    category_id = video["snippet"]["categoryId"]

    # contentDetails
    duration = video["contentDetails"]["duration"]
    licensed_content = video["contentDetails"]["licensedContent"]

    # status
    made_for_kids = video["status"]["madeForKids"]

    # statistics
    num_views = video["statistics"]["viewCount"]
    num_likes = video["statistics"].get("likeCount", None) # can be null
    num_comments = video["statistics"].get("commentCount", None) # can be null

    return [video_id, 
            publish_datetime, 
            channel_id, 
            video_title,
            video_description,
            channel_title,
            num_tags,
            category_id, 
            duration, 
            licensed_content, 
            made_for_kids, 
            num_views, 
            num_likes, 
            num_comments]    

def make_video_request(api_key, collected_at):
    videos_api_url = "https://www.googleapis.com/youtube/v3/videos"

    video_df = pd.DataFrame(columns=["collected_at", 
                                 "video_id", 
                                 "publish_datetime", 
                                 "channel_id",
                                 "video_title",
                                 "video_description",
                                 "channel_title",
                                 "num_tags", 
                                 "category_id",  
                                 "duration", 
                                 "licensed_content", 
                                 "made_for_kids", 
                                 "num_views", 
                                 "num_likes", 
                                 "num_comments"])
    
    # each API call gives 200 videos split into 4 pages, but you have to call again to see the other pages
    pageToken = ""

    while pageToken is not None:
        params = {
            "key": api_key,
            "part": "id, snippet, contentDetails, status, statistics",
            "chart": "mostPopular",
            "hl": "en",
            "regionCode": "US",
            "maxResults": 50,
            "pageToken": pageToken
        }
        
        response = requests.get(videos_api_url, params=params)

        if response.status_code == 200:
            response_json = response.json()

            for video in response_json["items"]:
                # add video details and datetime of request to end of video dataframe
                video_df.loc[len(video_df)] = [collected_at] + parse_video_json(video)

            # get nextPageToken with null safety
            pageToken = response_json.get("nextPageToken", None)

        else:
            print(f"response.status_code = {response.status_code}")
            pageToken = None

    return video_df

def parse_channel_json(channel):
    channel_id = channel["id"]

    # snippet
    channel_created_datetime = channel["snippet"]["publishedAt"]
    channel_name = channel["snippet"]["title"]

    # statistics
    channel_total_views = channel["statistics"].get("viewCount", None)
    channel_num_subscribers = channel["statistics"].get("subscriberCount", None)
    channel_num_videos = channel["statistics"].get("videoCount", None)

    return [channel_id, 
            channel_created_datetime,
            channel_name, 
            channel_total_views, 
            channel_num_subscribers,
            channel_num_videos] 
        
def make_channel_request(api_key, collected_at, channel_ids):
    channels_api_url = "https://www.googleapis.com/youtube/v3/channels"

    channel_df = pd.DataFrame(columns=["collected_at", 
                                "channel_id", 
                                "created_datetime", 
                                "channel_name"
                                "channel_total_views", 
                                "num_subscribers",
                                "num_videos"])

    # API limits 50 channels per call with no pages
    # split channels into chunks of 50
    channels_chunked = make_chunks(channel_ids, 50)

    for channels in channels_chunked:
        params = {
            "key": api_key,
            "part": "id, snippet, statistics",
            "id": ", ".join(channels),
            "maxResults": 50,
        }

        response = requests.get(channels_api_url, params=params)

        if response.status_code == 200:
            response_json = response.json()

            for channel in response_json["items"]:
                # add channel details and datetime of request to end of channel dataframe
                channel_df.loc[len(channel_df)] = [collected_at] + parse_channel_json(channel)

        else:
            print(f"response.status_code = {response.status_code}")

    return channel_df

def make_chunks(lst, n):
    # https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def make_db_connection(psql_pw):
    # connect to database
    host = "youtubeviewprediction.cd0c8oow2pnr.us-east-1.rds.amazonaws.com"
    port = 5432
    dbname = "YouTubeViewPrediction"
    user = "postgres"

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=psql_pw
        )

        # Create a cursor object using the cursor() method
        cursor = connection.cursor()

        # Execute a SQL query
        cursor.execute("SELECT version();")

        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return connection, cursor

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None, None

def insert_columns(df, cols, db_table, db_cursor, db_connection):
    data = (df.loc[:, cols]
                .to_records(index=False)
                .tolist())

    query = f"INSERT INTO {db_table} ({','.join(cols)}) VALUES %s"

    try:
        # Execute your SQL query
        psycopg2.extras.execute_values(db_cursor, query, data)
        
        # Commit the transaction
        db_connection.commit()
        print(f"Successfully inserted columns: {','.join(cols)}")
    except psycopg2.Error as e:
        # If an error occurs during execution, handle the exception
        print("Error executing query:", e)

def main():
    collected_at = datetime.now(timezone.utc) # YT API uses UTC timezone
    
    # load environment variables
    # put in bashrc because I run out of ram installing dotenv
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    # Videos ETL
    # get data on top 200 most popular videos currently
    # EXTRACT
    video_df = make_video_request(api_key, collected_at)
    
    # TRANSFORM
    # turn publish_datetime into datetime dtype
    video_df["publish_datetime"] = pd.to_datetime(video_df["publish_datetime"], format="ISO8601")
    video_df.rename(columns={"publish_datetime": "published_at"}, inplace=True)

    # parse duration into seconds
    video_df["duration"] = pd.to_timedelta(video_df["duration"]).apply(lambda x: x.seconds)
    video_df.rename(columns={"duration": "duration_seconds"}, inplace=True)

    # fill nulls
    video_df[["num_views", "num_likes", "num_comments"]] = video_df[["num_views", "num_likes", "num_comments"]].fillna(0)
    
    # turn to int
    video_df[["num_views", "num_likes", "num_comments"]] = video_df[["num_views", "num_likes", "num_comments"]].astype(dtype="int")

    # LOAD
    # connection, cursor = make_db_connection(psql_pw)

    # # database and tables already created in pgAdmin
    # # split video dataframe into video_fact and video_dim
    
    # # select cols to insert
    # video_fact_cols = ["collected_at", "video_id", "num_views", "num_likes", "num_comments"]
    # insert_columns(video_df, video_fact_cols, "video_fact", cursor, connection)
    
    # video_dim_cols = ["video_id", "channel_id", "video_title", "video_description", "num_tags", "duration_seconds", "licensed_content", "made_for_kids", "published_at", "category_id"]
    # insert_columns(video_df, video_dim_cols, "video_dim", cursor, connection)
    
    # Channels ETL
    # get YT channel metadata associated with top 200 videos
    unique_channel_ids = video_df["channel_id"].unique()

    # EXTRACT
    channel_df = make_channel_request(api_key, collected_at, unique_channel_ids)
    
    # TRANSFORM
    # Turn created_datetime into datetime
    channel_df["created_datetime"] = pd.to_datetime(channel_df["created_datetime"], format="ISO8601")

    # Fill NaNs and turn these into ints
    channel_df[["total_views", "num_subscribers", "num_videos"]] = channel_df[["total_views", "num_subscribers", "num_videos"]].fillna(0)
    channel_df[["total_views", "num_subscribers", "num_videos"]] = channel_df[["total_views", "num_subscribers", "num_videos"]].astype(dtype="int")
    print(channel_df.dtypes)

    # LOAD
    # database and tables already created in pgAdmin
    # split channels dataframe into channel_fact and channel_dim
    

if __name__ == "__main__":
    main()