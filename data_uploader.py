from datetime import date, datetime, timedelta
from time import time

import psycopg2 as sql
import pandas as pd
import requests
import io

# flags
DEBUG = False

# db constants data
DB_ADDRESS  = "localhost"
DB_NAME     = "postgres"
DB_USER     = "postgres"
DB_PASSWORD = "password_provvisoria"

# url data
FORMAT    = "csv"
STATIONS  = "2,4,6,8,9,15,22,23"
appa_data_url = f"https://bollettino.appa.tn.it/aria/opendata/{FORMAT}"

# update data
DAYS_TO_FETCH = 7

# UTIL FUNCTIONS
def connect_to_db(): # return type should be sql.connection but gives error
    """
    Connects to the db

    Returns:
        sql.connection: the connection object related to the db
    """
    return sql.connect(
        database = DB_NAME,
        host = DB_ADDRESS,
        user = DB_USER,
        password = DB_PASSWORD
    )

def format_dataframe(start_df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats a dataframe, removes the 'n.d.' values, adds leading 0s
    in front of single digit numbers and converts dates to datetime objs

    Args:
        start_df (pd.DataFrame): the dataframe to format

    Returns:
        pd.DataFrame: the final formatted dataframe
    """
    # convert all times to string and add 0 before
    #   single digit numbers to be parsed later
    start_df["Ora"] = start_df["Ora"].astype(str).str.zfill(2)
    
    # replace 24 hour with 00 so python will recognise it
    start_df["Ora"] = start_df["Ora"].replace("24", "00")
    
    # replace n.d. values with -1 values
    start_df = start_df.replace("n.d.", -1)

    return start_df

def build_value_list(dataframe: pd.DataFrame) -> str:
    """
    Builds the value list to be put in an INSERT sql statement

    Args:
        dataframe (pd.DataFrame): the dataframe where to extract the data

    Returns:
        str: the string to be put as values
    """
    output = ""
    for _, row in dataframe.iterrows(): # _ is index
        # convert date and time in a single object
        timestamp = datetime.strptime(
            row['Data'] + ' ' + row['Ora'],
            "%Y-%m-%d %H"
        )
    
        output += f", ('{row['Stazione']}', '{row['Inquinante']}', '{timestamp.isoformat(' ')}', {row['Valore']})"
        # timestamp is in ISO format (YYYY-MM-DD) + (HH:MM:SS.mmmmmm) with a space as separator
    output = output[1:] # remove first ',' to prevent errors

    return output

def execute_insert(cur, insert_data: str) -> None: # cur type should be sql.cursor but gives error
    """
    Executes the INSERT sql query with the insert_data given

    Args:
        cur (sql.cursor): the cursor object where to make the operation
        insert_data (str): the data to insert
    """
    cur.execute(
        "INSERT INTO appa_data (" +
            "stazione, " +
            "inquinante, " +
            "ts, " +
            "valore" +
        ") VALUES " +
            insert_data +
        " ON CONFLICT DO NOTHING" # ignore the insert if the key (stazione, inquinante, ts) already exists
    )

# DATABASE INITAL POPULATOR
def populate_db(base_url: str) -> None:
    """
    Populates the db with all data it can find on appa's website

    Args:
        base_url (str): the starting url
    """
    if DEBUG: population_start = time()

    connection = connect_to_db()
    cur = connection.cursor()

    i = 0
    while True:
        # calculate time range to request (max 90 days allowed by server)
        date_start  = date.today() - timedelta(days = 90*i)
        date_end    = date.today() - timedelta(days = 90*(i + 1))
        date_string = date_end.strftime("%Y-%m-%d") + "," + date_start.strftime("%Y-%m-%d")
        temp_url = base_url + f"/{date_string}/{STATIONS}"

        if DEBUG: 
            print(f"fetching data segment: {date_string}")
            fetch_start = time()

        # get the data
        raw_data = requests.get(temp_url).content

        # convert it into a dataframe
        temp_df = pd.read_csv(
            io.StringIO(raw_data.decode("windows 1252"))
        )

        # stop asking data if there is no more
        if len(temp_df) == 0: break

        # increment index to be used in next cycle
        i += 1

        # correct some values in dataframe
        temp_df = format_dataframe(temp_df)

        # build the value to insert into the db
        insert_data = build_value_list(temp_df)

        if DEBUG:
            time_delta = time() - fetch_start
            print(f"fetch finished ({time_delta // 60}m {time_delta % 60}s)")

        execute_insert(cur, insert_data)

    connection.commit()
    connection.close()
    
    if DEBUG:
        time_delta = time() - population_start
        print(f"finished ({time_delta // 60}m {time_delta % 60}s)")

def update_db(base_url: str) -> None:
    """
    Updates the db with the last days data, ignoring duplicates

    Args:
        base_url (str): the starting url
    """
    date_start  = date.today()
    date_end    = date.today() - timedelta(days = DAYS_TO_FETCH)
    date_string = date_end.strftime("%Y-%m-%d") + "," + date_start.strftime("%Y-%m-%d")

    if DEBUG:
        print(f"fetching data segment: {date_string}")
        population_start = time()

    base_url = base_url + f"/{date_string}/{STATIONS}"

    connection = connect_to_db()
    cur = connection.cursor()

    # get the data
    raw_data = requests.get(base_url).content
    
    # convert it into a dataframe
    temp_df = pd.read_csv(
        io.StringIO(raw_data.decode("windows 1252"))
    )

    # correct some values in dataframe
    temp_df = format_dataframe(temp_df)

    # build the value to insert into the db
    insert_data = build_value_list(temp_df)

    execute_insert(cur, insert_data)

    connection.commit()
    connection.close()
    
    if DEBUG:
        time_delta = time() - population_start
        print(f"finished ({time_delta // 60}m {time_delta % 60}s)")

# script runtime
if __name__ == "__main__":
    #populate_db(appa_data_url)
    update_db(appa_data_url)
