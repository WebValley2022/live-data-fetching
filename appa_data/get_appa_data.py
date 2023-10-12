import requests
import pytz
from datetime import datetime, timezone
from datetime import date
import psycopg2 
import time

FORMAT = "%Y-%m-%dT%H:%M:%SZ"
stations_format = ["Parco S. Chiara", "Via Bolzano"]

conn = psycopg2.connect(user="postgres", password="postgres", host="localhost", database="webvalley2022")

pollutants = {
    'Parco S. Chiara' : {
                'NO2': 1843,
                'PM2.5':3164,
                'PM10':3716,
                'SO2':4289,
                'O3':4954,
    },
    'Via Bolzano': {
        'CO': 677,
        'NO2': 1792,
        'PM10': 3724,
    }
}
def get_url(pollutant):
    return f"https://airquality-frost.k8s.ilt-dmz.iosb.fraunhofer.de/v1.1/Datastreams({pollutant})/Observations"

def delete_table(conn):
    curr = conn.cursor()
    curr.execute("DROP TABLE IF EXISTS public.appa_data")
    conn.commit()
    curr.close()
    
def chenge_pullutants_name(conn):
    curr = conn.cursor()
    rename = {
        "Biossido di Azoto" : 'NO2',
        "Biossido Zolfo" : 'SO2',
        "Ozono" : 'O3',
        "Ossido di Carbonio" : 'CO',
    }
    
    for key, item in rename.items():
        query = f"""
            UPDATE appa_data
            SET inquinante = '{item}' 
            WHERE inquinante = '{key}';
        """
        curr.execute(query)
        conn.commit()
        
    curr.close()

def create_table(conn):
    curr = conn.cursor()
    curr.execute(
        """
        CREATE TABLE IF NOT EXISTS public.appa_data
        (
            id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
            stazione text COLLATE pg_catalog."default" NOT NULL,
            inquinante text COLLATE pg_catalog."default" NOT NULL,
            ts timestamp(0) with time zone NOT NULL,
            valore bigint NOT NULL,
            CONSTRAINT appa_data_pkey PRIMARY KEY (id),
            CONSTRAINT appa_data_stazione_inquinante_ts_key UNIQUE (stazione, inquinante, ts)
        )
        """)
    conn.commit()
    curr.close()
        

def populate_db(conn):
    
    
    curr = conn.cursor()
   
    for station in stations_format:
        for pollutant, datastream in pollutants[station].items():
            start_date = date(2018,1,1)
            my_time = datetime.min.time()
            start_date = datetime.combine(start_date, my_time)
            while(start_date<datetime.now()):
                
                skip = False
                start_str= datetime.strftime(start_date,format=FORMAT)
                end_date = start_date.replace(year= start_date.year + 1)
                end_str = datetime.strftime(end_date,format=FORMAT)
                payload = {
                '$resultFormat': 'dataArray',
                '$select' : 'phenomenonTime, result',
                '$orderby': 'phenomenonTime asc',
                '$filter': f'(phenomenonTime gt {start_str}) and (phenomenonTime lt {end_str})',
                '$top' : '10000'
                
                }
        
                r = requests.get(get_url(datastream), params=payload)
                r_dict = r.json()
                
                try:
                    print (f"size poll {pollutant} in {station}: {len(r_dict.get('value')[0].get('dataArray'))}")
                except:
                    print (f"NO DATA for {pollutant} in {station} with range '{start_str}' -> '{end_str}'")
                    skip = True
                
                if not skip:
                    list_values = []
                    for values in r_dict.get('value')[0].get('dataArray'):
                       # Stringa di esempio
                        string = values[0].split('/')[1]
                        
                        list_values.append((station, pollutant, string,values[1]))
                        #valore_pollutant = values[1]
                        
                    values = str(list_values).replace('[','').replace(']','')
                    
                    INSERT = f"""
                        INSERT INTO public.appa_data(
                            stazione, inquinante, ts, valore)
                        VALUES {values} ON CONFLICT (stazione, inquinante, ts) DO UPDATE
                            SET valore = EXCLUDED.valore;
                    """
                    try:
                        curr.execute(INSERT)
                        conn.commit()
                    except (Exception, psycopg2.DatabaseError) as error:
                        print("Error: %s" % error)
                        conn.rollback()
                        curr.close()
                    print(f"Inquinante {pollutant} in {station} in range '{start_str}' -> '{end_str}'  done")
                
                start_date = start_date.replace(year= start_date.year + 1)
                
                               
def getting_data(conn):
    curr = conn.cursor()
    payload = {
                '$resultFormat': 'dataArray',
                '$select' : 'phenomenonTime, result',
                '$orderby': 'phenomenonTime desc',
                '$top' : '10'
                }
     

    try:
        while(True):
            for station in stations_format:
                for pollutant, datastream in pollutants[station].items():
                    skip = False
                    r = requests.get(get_url(datastream), params=payload)
                    try:
                        r_dict = r.json()
                    except:
                        skip=True
                        print(f'ERROR on: {r}')
                    
                    try:
                       size = len(r_dict.get('value')[0].get('dataArray'))
                    except:
                        print (f"NO DATA for {pollutant} in {station} in last 10 hours")
                        skip = True
                    
                    if not skip:
                        list_values = []
                        for values in r_dict.get('value')[0].get('dataArray'):
                            # Stringa di esempio
                            string = values[0].split('/')[1]
                            
                            list_values.append((station, pollutant, string, values[1]))
                            
                        values = str(list_values).replace('[','').replace(']','')
                        
                        INSERT = f"""
                            INSERT INTO public.appa_data(
                                stazione, inquinante, ts, valore)
                            VALUES {values} ON CONFLICT DO NOTHING;
                        """
                        try:
                            curr.execute(INSERT)
                            conn.commit()
                        except (Exception, psycopg2.DatabaseError) as error:
                            print("Error: %s" % error)
                            conn.rollback()
                            curr.close()
                        print(f"Inquinante {pollutant} in {station} done")
                        
            print(f"Acquisition time: {datetime.now()}")
            time.sleep(21600) # 6 hours
            #time.sleep(15)
    except KeyboardInterrupt:
        curr.close()
        conn.close()
        print("Connection closed")
        exit(1)
                
            
            
def main():
    aa= 1688654597717 / 1000.0
    
    print(datetime.utcfromtimestamp(aa))
    print(datetime.fromtimestamp(aa))
    exit_key = True
    while(exit_key):
        menu = input("""
                     1- delete table appa
                     2- create table appa
                     3- populate db since 2018
                     4- get appa data in real time
                     5- use acronyms as pollutants name
                     Q- quit
                     """)
        print(menu)
        if menu == '1':
            #delete_table(conn)
            pass
        if menu == '2':
            create_table(conn)  
        if menu == '3':
            populate_db(conn)
        if menu == '4':
            getting_data(conn)
        if menu == '5':
            chenge_pullutants_name(conn)
        if menu == 'q':
            conn.close()
            exit_key=False
            

if __name__ == "__main__":
    main()