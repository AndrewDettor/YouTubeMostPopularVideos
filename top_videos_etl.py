import os
from dotenv import load_dotenv
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

def make_video_request():
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
    # assume API calls are made at the same exact time
    script_timestamp = datetime.now(timezone.utc) # YT API uses UTC timezone

    # load environment variables from .env file
    load_dotenv("environment_variables.env")
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    videos_api_url = "https://www.googleapis.com/youtube/v3/videos"

    video_df = make_video_request()

    

if __name__ == "__main__":
    main()