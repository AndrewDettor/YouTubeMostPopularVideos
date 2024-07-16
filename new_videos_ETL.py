import os
import requests
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv
from db_utils import make_db_connection, insert_rows, find_values_not_in_col

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

def main():
    collected_at = datetime.now(timezone.utc) # YT API uses UTC timezone
    
    # Load environment variables from .env file
    load_dotenv("environment_variables.env")
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    # Connect to AWS RDS through SSH tunnel
    conn, cursor, tunnel = make_db_connection(psql_pw)

    # Videos ETL
    # get data on top 200 most popular videos currently
    # EXTRACT
    video_df = make_video_request(api_key, collected_at)

    # check which video_ids aren't already in video_dim table
    values = video_df.loc[:, "video_id"].tolist()
    video_ids_not_in_table = find_values_not_in_col(values, "video_id", "video_dim", cursor)

    # filter the dataframe to only be new videos
    video_df = video_df[video_df["video_id"].isin(video_ids_not_in_table)]

    # print(f"video_df.shape[0]: {video_df.shape[0]}\n")
    
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

    # insert into dim table
    insert_rows(video_df, 
                ["video_id", "channel_id", "video_title", "video_description", "num_tags", "duration_seconds", "licensed_content", "made_for_kids", "published_at", "category_id"], 
                "video_dim", 
                cursor, 
                conn)

    # insert into fact table
    insert_rows(video_df, 
                ["collected_at", "video_id", "num_views", "num_likes", "num_comments"], 
                "video_fact", 
                cursor, 
                conn)

    # get YT channels of the videos that were just uploaded
    unique_channel_ids = video_df["channel_id"].unique()

    # write to a text file to be used by the next ETL
    with open("C:\\Users\\detto\\Documents\\YouTubeViewPrediction\\new_ETLs\\unique_channel_ids.txt", "w") as file:
        for channel_id in unique_channel_ids:
            file.write(channel_id + "\n")
    
    print(f"Created text file of {len(unique_channel_ids)} channel ids for new_channels_ETL")

    conn.close()
    cursor.close()
    tunnel.stop()

if __name__ == "__main__":
    main()