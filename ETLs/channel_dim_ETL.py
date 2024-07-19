import os
import requests
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from db_utils import make_db_connection, insert_rows, find_values_not_in_col

def channel_dim_api_request(api_key, channel_ids):
    channels_api_url = "https://www.googleapis.com/youtube/v3/channels"

    channel_df = pd.DataFrame(columns=["channel_id", 
                                       "created_datetime", 
                                       "channel_name"])

    # API limits 50 channels per call with no pages
    # split channels into chunks of 50
    channels_chunked = make_chunks(channel_ids, 50)

    for channels in channels_chunked:
        params = {
            "key": api_key,
            "part": "id, snippet",
            "id": ", ".join(channels),
            "maxResults": 50,
        }

        response = requests.get(channels_api_url, params=params)

        if response.status_code == 200:
            response_json = response.json()

            for channel in response_json["items"]:
                # add channel details and datetime of request to end of channel dataframe
                channel_df.loc[len(channel_df)] = parse_channel_dim_json(channel)

        else:
            print(f"response.status_code = {response.status_code}")

    return channel_df

def parse_channel_dim_json(channel):
    # id
    channel_id = channel["id"]

    # snippet
    created_datetime = channel["snippet"]["publishedAt"]
    channel_name = channel["snippet"]["title"]

    return [channel_id, 
            created_datetime,
            channel_name]
        
def make_chunks(lst, n):
    # https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def main():
    print("Starting channel_dim_ETL")
    
    # Load environment variables from .bashrc
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    # Connect to RDS from EC2 instance
    conn, cursor = make_db_connection(psql_pw)

    # Get YouTube channel ids from the last ETL
    # Read the values from the text file into a list
    with open("/home/ec2-user/YouTubeViewPrediction/ETLs/unique_channel_ids.txt", 'r') as file:
        unique_channel_ids = file.readlines()

    # Remove any trailing newline characters
    unique_channel_ids = [value.strip() for value in unique_channel_ids]

    # check which channel_ids aren't already in channel_dim table
    channel_ids_not_in_table = find_values_not_in_col(unique_channel_ids, "channel_id", "channel_dim", cursor)
    print(f"\tRequesting YT API current info on {len(channel_ids_not_in_table)} channel ids")

    # EXTRACT
    channel_df = channel_dim_api_request(api_key, channel_ids_not_in_table)
    
    # TRANSFORM
    # Turn created_datetime into datetime
    channel_df["created_datetime"] = pd.to_datetime(channel_df["created_datetime"], format='ISO8601')
    # %Y-%m-%dT%H:%M:%S%z
    
    # LOAD
    # database and tables already created in pgAdmin
    # insert into dim table
    insert_rows(channel_df, 
                channel_df.columns,  
                "channel_dim", 
                cursor, 
                conn)

    conn.close()
    cursor.close()

if __name__ == "__main__":
    main()