import os
import requests
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from db_utils import make_db_connection

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
        
def make_chunks(lst, n):
    # https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def main():
    collected_at = datetime.now(timezone.utc) # YT API uses UTC timezone
    
    # Load environment variables from .env file
    load_dotenv("environment_variables.env")
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    # Connect to AWS RDS through SSH tunnel
    conn, cursor, tunnel = make_db_connection(psql_pw)

    # get all channel_ids in channel_dim table

    # make api request for current info on all the channels

    # do the same data cleaning as new_channels_ETL

    # load into channel_fact table

    conn.close()
    cursor.close()
    tunnel.stop()

if __name__ == "__main__":
    main()