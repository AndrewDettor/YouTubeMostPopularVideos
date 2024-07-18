from sshtunnel import SSHTunnelForwarder
import psycopg2
import psycopg2.extras
import pandas as pd

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
        print("You are connected to - ", record)
        return conn, cursor, tunnel

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        tunnel.stop()

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
        print(f"Successfully inserted {num_rows} rows into {table_name}")
    except psycopg2.Error as e:
        # If an error occurs during execution, handle the exception
        print("Error executing query in insert_rows:", e)

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
        print("Error executing query in find_values_not_in_col:", e)

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
        print("Error executing query in find_values_not_in_col:", e)