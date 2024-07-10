import os
import requests
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import math
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
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
                                "channel_name",
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

def parse_videoCategories_json(category):
    id = category["id"]
    name = category["snippet"]["title"]

    return [id, name]

def make_categories_request(api_key):
    videoCategories_api_url = "https://www.googleapis.com/youtube/v3/videoCategories"
    
    videoCategories_df = pd.DataFrame(columns=["category_id","category_name"])

    params = {
        "key": api_key,
        "part": "snippet",
        "regionCode": "US",
    }

    response = requests.get(videoCategories_api_url, params=params)

    if response.status_code == 200:
        response_json = response.json()

        for category in response_json["items"]:
            # add channel details and datetime of request to end of video dataframe
            videoCategories_df.loc[len(videoCategories_df)] = parse_videoCategories_json(category)

    else:
        print(f"response.status_code = {response.status_code}")

    return videoCategories_df

def make_chunks(lst, n):
    # https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def make_db_connection(psql_pw):
    # SSH parameters
    bastion_host = 'ec2-34-224-93-62.compute-1.amazonaws.com'
    bastion_user = 'ec2-user'
    bastion_key = 'C:\\Users\\detto\\Documents\\ec2-key-pair.pem'

    # RDS parameters
    rds_host = 'youtubeviewprediction.cd0c8oow2pnr.us-east-1.rds.amazonaws.com'
    rds_user = 'postgres'
    rds_password = psql_pw
    rds_database = 'YouTubeViewPrediction'
    rds_port = 5432

    try:
        # Create an SSH tunnel
        tunnel = SSHTunnelForwarder(
            (bastion_host, 22),
            ssh_username=bastion_user,
            ssh_pkey=bastion_key,
            remote_bind_address=(rds_host, rds_port),
            local_bind_address=('localhost', 6543)  # Choose a local port for the tunnel
        )

        # Start the tunnel
        tunnel.start()

        # Connect to PostgreSQL through the tunnel
        conn = psycopg2.connect(
            database=rds_database,
            user=rds_user,
            password=rds_password,
            host=tunnel.local_bind_host,
            port=tunnel.local_bind_port
        )

        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Execute a SQL query
        cursor.execute("SELECT version();")

        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return conn, cursor

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

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
        print(f"Successfully inserted columns: {', '.join(cols)}")
    except psycopg2.Error as e:
        # If an error occurs during execution, handle the exception
        print("Error executing query:", e)

def main():
    collected_at = datetime.now(timezone.utc) # YT API uses UTC timezone
    
    # Load environment variables from .env file
    load_dotenv("environment_variables.env")
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    # Connect to AWS RDS through SSH tunnel
    connection, cursor = make_db_connection(psql_pw)

    # Videos ETL
    # get data on top 200 most popular videos currently
    # EXTRACT
    video_df = make_video_request(api_key, collected_at)
    
    # TRANSFORM
    # turn publish_datetime into datetime dtype
    video_df["publish_datetime"] = pd.to_datetime(video_df["publish_datetime"])
    video_df.rename(columns={"publish_datetime": "published_at"}, inplace=True)

    # parse duration into seconds
    video_df["duration"] = pd.to_timedelta(video_df["duration"]).apply(lambda x: x.seconds)
    video_df.rename(columns={"duration": "duration_seconds"}, inplace=True)

    # fill nulls
    video_df[["num_views", "num_likes", "num_comments"]] = video_df[["num_views", "num_likes", "num_comments"]].fillna(0)
    
    # turn to int
    video_df[["num_views", "num_likes", "num_comments"]] = video_df[["num_views", "num_likes", "num_comments"]].astype(dtype=int)

    # LOAD
    # database and tables already created in pgAdmin
    # split video dataframe into video_fact and video_dim
    
    # select cols to insert
    video_fact_cols = ["collected_at", "video_id", "num_views", "num_likes", "num_comments"]
    insert_columns(video_df, video_fact_cols, "video_fact", cursor, connection)
    
    video_dim_cols = ["video_id", "channel_id", "video_title", "video_description", "num_tags", "duration_seconds", "licensed_content", "made_for_kids", "published_at", "category_id"]
    insert_columns(video_df, video_dim_cols, "video_dim", cursor, connection)
    
    # Channels ETL
    # get YT channel metadata associated with top 200 videos
    unique_channel_ids = video_df["channel_id"].unique()

    # EXTRACT
    channel_df = make_channel_request(api_key, collected_at, unique_channel_ids)
    
    # TRANSFORM
    # Turn created_datetime into datetime
    channel_df["created_datetime"] = pd.to_datetime(channel_df["created_datetime"])
    
    # Fill NaNs and turn these into ints
    channel_df[["channel_total_views", "num_subscribers", "num_videos"]] = channel_df[["channel_total_views", "num_subscribers", "num_videos"]].fillna(0)
    channel_df[["channel_total_views", "num_subscribers", "num_videos"]] = channel_df[["channel_total_views", "num_subscribers", "num_videos"]].astype(dtype=np.int64)
    
    # LOAD
    # database and tables already created in pgAdmin
    # split channels dataframe into channel_fact and channel_dim
    channel_fact_cols = ["collected_at", "channel_id", "channel_total_views", "num_subscribers", "num_videos"]
    insert_columns(channel_df, channel_fact_cols, "channel_fact", cursor, connection)

    channel_dim_cols = ["channel_id", "channel_name", "created_datetime"]
    insert_columns(channel_df, channel_dim_cols, "channel_dim", cursor, connection)

    # Categories ETL
    # EXTRACT
    videoCategories_df = make_categories_request(api_key)

    # LOAD
    # database and table already created in pgAdmin
    insert_columns(videoCategories_df, ["category_id", "category_name"], "categories_dim", cursor, connection)

if __name__ == "__main__":
    main()