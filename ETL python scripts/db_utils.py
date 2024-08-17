import psycopg2
import psycopg2.extras
import pandas as pd

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
        # print("\tYou are connected to - ", record)
        return connection, cursor

    except (Exception, psycopg2.Error) as error:
        print("\tError while connecting to PostgreSQL", error)
        return None, None

def insert_rows(df, cols, table_name, cursor, conn):
    num_rows = df.shape[0]

    data = (df.loc[:, cols]
                .to_records(index=False)
                .tolist())

    query = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES %s"

    try:
        # Execute SQL query
        psycopg2.extras.execute_values(cursor, query, data)
        
        # Commit the transaction
        conn.commit()
        print(f"\tSuccessfully inserted {num_rows} rows into {table_name}")
    except psycopg2.Error as e:
        # If an error occurs during execution, handle the exception
        print("\tError executing query in insert_rows:", e)

def find_values_not_in_col(values, col, table_name, cursor):    
    query = f"SELECT {col} \
            FROM {table_name} \
            WHERE {col} = ANY(%s)"

    try:
        # Execute SQL query
        cursor.execute(query, (values,))
        
        # Fetch all rows from the query
        existing_values = cursor.fetchall()

        # de-nest the list
        existing_values = [row[0] for row in existing_values]

        # print(f"existing_values: {existing_values}\n")
        # print(f"values: {values}\n")

        not_in_col = set(values) - set(existing_values)
        # print(f"not_in_col: {not_in_col}\n")
        return list(not_in_col)
        
    except psycopg2.Error as e:
        # If an error occurs during execution, handle the exception
        print("\tError executing query in find_values_not_in_col:", e)

def select_all_from_col(col, table_name, cursor):
    query = f"SELECT {col} \
            FROM {table_name}"
    
    try:
        # Execute SQL query
        cursor.execute(query)
        
        # Fetch all rows from the query
        # if there's too many rows this could break
        results = cursor.fetchall()

        # de-nest the list
        results = [row[0] for row in results]

        return results
        
    except psycopg2.Error as e:
        # If an error occurs during execution, handle the exception
        print("\tError executing query in find_values_not_in_col:", e)