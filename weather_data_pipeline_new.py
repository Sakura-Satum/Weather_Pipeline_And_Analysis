import requests
import re
import schedule
from config import config
import psycopg2

import time
#%%


def pipe_line():
    data_dic = {}

    conn_details = None
    try:
        params = config()
        print('Connecting to the postgreSQL database ...')
        conn_details = psycopg2.connect(**params)
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

    key = '6b3bb5db42d3499b939131130231708'
    locations = ['London', 'Birmingham', 'Manchester', 'Glasgow', 'Sheffield',
                    'Leeds', 'Bristol', 'Nottingham',  'Liverpool'
                    ]

    for location in locations:
        url = f'http://api.weatherapi.com/v1/forecast.json?key={key}&q={location}&days=5&aqi=no&alerts=no'
        response = requests.get(url)
        data_dic[location] = response.json()
    
    # dc.conn_details.autocommit = True
    cursor = conn_details.cursor()

    table_creation = '''
                        CREATE TABLE IF NOT EXISTS student.ss_weather_locations (
                        loc_key VARCHAR ( 30 ) PRIMARY KEY,
                        region VARCHAR ( 50 ) NOT NULL,
                        country VARCHAR ( 30 ) NOT NULL,
                        lat float8,
                        lon float8
                        );
                    '''

    cursor.execute(table_creation)
    conn_details.commit()

    for location in locations:
        loc_info = data_dic[location]['location']
        sql = f'''
                    INSERT INTO student.ss_weather_locations
                    SELECT
                    '{location.lower()}',
                    '{loc_info['region']}',
                    '{loc_info['country']}',
                    {loc_info['lat']},
                    {loc_info['lon']}
                    WHERE
                    NOT EXISTS (SELECT loc_key
                           FROM student.ss_weather_locations
                           WHERE loc_key = '{location.lower()}');
              '''
        cursor.execute(sql)
        conn_details.commit()

    table_creation = '''
                        CREATE TABLE IF NOT EXISTS student.ss_weather_detail (
                        weather_key  VARCHAR ( 23 ) PRIMARY KEY,
                        loc_key VARCHAR ( 30 ),
                        updated_date date  NOT NULL,
                        updated_time time NOT NULL,
                        iconn VARCHAR ( 50 ) NOT NULL,
                        condition VARCHAR ( 30 ) ,
                        temp_c float8,
                        wind_mph float8,
                        humidity float8,
                        feelslike_c float8,
                        wind_dir VARCHAR (3),

                        FOREIGN KEY (loc_key)
                        REFERENCES student.ss_weather_locations (loc_key)
                        );
                    '''
    cursor.execute(table_creation)
    conn_details.commit()

    for location in locations:
        cur_info = data_dic[location]['current']
        date_time = data_dic[location]['current']['last_updated'].split(' ')
        wkey = f"{location.lower()}_{re.sub('[^0-9]', '', cur_info['last_updated'])}"

        sql = f'''
              INSERT INTO student.ss_weather_detail
              SELECT
              '{wkey}',
              '{location.lower()}',
              '{date_time[0]}',
              '{date_time[1]}',
              '{cur_info['condition']['icon']}',
              '{cur_info['condition']['text']}',
              '{cur_info['temp_c']}',
              {cur_info['wind_mph']},
              {cur_info['humidity']},
              {cur_info['feelslike_c']},
              '{cur_info['wind_dir']}'
              WHERE
                    NOT EXISTS (SELECT weather_key
                           FROM student.ss_weather_detail
                           WHERE loc_key = '{wkey}');
              '''
        cursor.execute(sql)
        conn_details.commit()

    conn_details.commit()
    cursor.close()
    print("hello3")


#%%

schedule.every(20).minutes.do(pipe_line)
while True:
    # Run all jobs that are scheduled to run.
    schedule.run_pending()
    time.sleep(1)