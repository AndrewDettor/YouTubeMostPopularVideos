import os
import requests
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import math
import psycopg2

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

def make_video_request(api_key):
    script_timestamp = datetime.now(timezone.utc) # YT API uses UTC timezone

    videos_api_url = "https://www.googleapis.com/youtube/v3/videos"

    video_df = pd.DataFrame(columns=["script_timestamp", 
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
                video_df.loc[len(video_df)] = [script_timestamp] + parse_video_json(video)

            # get nextPageToken with null safety
            pageToken = response_json.get("nextPageToken", None)

        else:
            print(f"response.status_code = {response.status_code}")
            pageToken = None

    return video_df

def main():
    # load environment variables
    # put in bashrc because I run out of ram installing dotenv
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    # EXTRACT
    video_df = make_video_request(api_key)
    
    # TRANSFORM
    # turn publish_datetime into datetime format
    video_df["publish_datetime"] = pd.to_datetime(video_df["publish_datetime"], format="ISO8601")

    # parse duration into seconds
    video_df["duration"] = pd.to_timedelta(video_df["duration"]).apply(lambda x: x.seconds)
    video_df.rename(columns={"duration": "duration_seconds"}, inplace=True)

    # fill nulls
    video_df[["num_views", "num_likes", "num_comments"]] = video_df[["num_views", "num_likes", "num_comments"]].fillna(0)
    
    # turn to int
    video_df[["num_views", "num_likes", "num_comments"]] = video_df[["num_views", "num_likes", "num_comments"]].astype(dtype="int")

    # LOAD
    # connect to database
    host = "youtubeviewprediction.cd0c8oow2pnr.us-east-1.rds.amazonaws.com"
    port = 5432
    database = "postgres"
    user = "postgres"

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
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

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    # create tables

    

if __name__ == "__main__":
    main()