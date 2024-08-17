import os
import requests
import pandas as pd
from db_utils import make_db_connection, insert_rows, find_values_not_in_col

def categories_dim_api_request(api_key):
    categories_api_url = "https://www.googleapis.com/youtube/v3/videoCategories"
    
    categories_df = pd.DataFrame(columns=["category_id",
                                          "category_name"])

    params = {
        "key": api_key,
        "part": "snippet",
        "regionCode": "US",
    }

    response = requests.get(categories_api_url, params=params)

    if response.status_code == 200:
        response_json = response.json()

        for category in response_json["items"]:
            # add channel details and datetime of request to end of video dataframe
            categories_df.loc[len(categories_df)] = parse_categories_dim_json(category)

    else:
        print(f"response.status_code = {response.status_code}")

    return categories_df

def parse_categories_dim_json(category):
    # id
    id = category["id"]

    # snippet
    name = category["snippet"]["title"]

    return [id, name]

def main():
    print("Starting categories_dim_ETL")
    
    # Load environment variables from .bashrc
    api_key = os.getenv("API_KEY")
    psql_pw = os.getenv("PSQL_PW")

    # Connect to RDS from EC2 instance
    conn, cursor = make_db_connection(psql_pw)

    # EXTRACT
    categories_df = categories_dim_api_request(api_key)

    # check which videoCategory_ids aren't already in categories_dim table
    # cast category_id to int to work with util function
    categories_df["category_id"] = categories_df["category_id"].astype(int)

    values = categories_df.loc[:, "category_id"].tolist()

    category_ids_not_in_table = find_values_not_in_col(values, "category_id", "categories_dim", cursor)

    # filter the dataframe to only be new videos
    categories_df = categories_df[categories_df["category_id"].isin(category_ids_not_in_table)]

    # LOAD
    # database and table already created in pgAdmin

    # insert into dim table
    insert_rows(categories_df, 
                categories_df.columns, 
                "categories_dim", 
                cursor, 
                conn)

    conn.close()
    cursor.close()

if __name__ == "__main__":
    main()