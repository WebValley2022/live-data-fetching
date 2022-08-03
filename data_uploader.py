from datetime import date, datetime, timedelta

import psycopg2 as sql
import pandas as pd
import requests
import io

# db constants data
DB_ADDRESS  = "localhost"
DB_NAME     = "postgres"
DB_USER     = "postgres"
DB_PASSWORD = "password_provvisoria"

# url data
FORMAT    = "csv"
STATIONS  = "2,4,6,8,9,15,22,23"
appa_data_url = f"https://bollettino.appa.tn.it/aria/opendata/{FORMAT}"

# DATABASE INITAL POPULATOR
def populate_db(base_url: str) -> None:

    connection = sql.connect(
        database = DB_NAME,
        host = DB_ADDRESS,
        user = DB_USER,
        password = DB_PASSWORD
    )
    cur = connection.cursor()

    i = 0
    while True:
        # calculate time range to request (max 90 days allowed by server)
        date_start  = date.today() - timedelta(days = 90*i)
        date_end    = date.today() - timedelta(days = 90*(i + 1))
        date_string = date_end.strftime("%Y-%m-%d") + "," + date_start.strftime("%Y-%m-%d")
        temp_url = base_url + f"/{date_string}/{STATIONS}"

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

        # convert all times to string and add 0 before
        #   single digit numbers to be parsed later
        temp_df["Ora"] = temp_df["Ora"].astype(str).str.zfill(2)

        # replace 24 hour with 00 so python will recognise it
        temp_df["Ora"] = temp_df["Ora"].replace("24", "00")

        # replace n.d. values with -1 values
        temp_df = temp_df.replace("n.d.", -1)

        for index, row in temp_df.iterrows():
            print(row)

            # convert date and time in a single object
            timestamp = datetime.strptime(
                row['Data'] + ' ' + row['Ora'],
                "%Y-%m-%d %H"
            )
    
            cur.execute(
                "INSERT INTO appa_data (" +
                        "stazione, " +
                        "inquinante, " +
                        "ts, " +
                        "valore" +
                    ") VALUES (" +
                        f"'{row['Stazione']}', " +          # station name
                        f"'{row['Inquinante']}', " +        # pollutant name
                        f"'{timestamp.isoformat(' ')}', " + # timestamp in ISO format (YYYY-MM-DD) + (HH:MM:SS.mmmmmm) with a space as separator
                        f"{row['Valore']}" +                # value
                    ") ON CONFLICT DO NOTHING"              # ignore the insert if the key (stazione, inquinante, ts) already exists
            )
    
    connection.commit()
    connection.close()

populate_db(appa_data_url)
