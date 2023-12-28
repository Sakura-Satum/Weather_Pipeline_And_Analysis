import matplotlib.pyplot as plt
import matplotlib
import psycopg2
import pandas as pd
from datetime import date,datetime
from config import config
matplotlib.use('TkAgg')

#%%

locations = ['London', 'Birmingham', 'Manchester', 'Glasgow', 'Sheffield',
             'Leeds', 'Bristol', 'Nottingham',  'Liverpool'
             ]
location = input(f'''
                    What city from 
                    
                    {locations} 
                    
                    do you wish to discover about the monthly trend in daily temperature?
                    ''').title()

#%%
conn_details = None
try:
    params = config()
    print('Connecting to the postgreSQL database ...')
    conn_details = psycopg2.connect(**params)
except(Exception, psycopg2.DatabaseError) as error:
    print(error)

conn_details.autocommit = True
cursor = conn_details.cursor()

#%%
txt_x_label = 'Days In Month'
txt_title_label = 'Temperature Over The Month'

if location in locations:
    locations = [location]
    txt_title_label = f'Temperature in {location} Over The Month'
    today_day = int(str(date.today()).split('-')[-1])
    date = input(f'''
                    What is the date of the month do you wish to discover about the hourly temperature?
                    ''')

    if date == '':
        date = 0
    else:
        date = int(date)

    cursor.execute(
        f'''
            SELECT
                date_part('day', wd.updated_date) as updated_date,
                round(avg(wd.temp_c) :: numeric,2) as temp
            FROM student.ss_weather_detail wd
                JOIN ss_weather_locations swl
                    ON wd.loc_key = swl.loc_key
            WHERE date_part('year', wd.updated_date) = date_part('year', CURRENT_DATE)
                and date_part('month', wd.updated_date) = date_part('month', CURRENT_DATE)
                and wd.loc_key = '{location.lower()}'
            GROUP BY swl.loc_key,updated_date;
    ''')

    if date <= today_day and date > 0:
        txt_x_label = f'Hours On {date}/{datetime.now().month}/{datetime.now().year}'
        txt_title_label = f'Hourly Temperature On {date}/{datetime.now().month}/{datetime.now().year}'
        cursor.execute(
            f'''
            SELECT
                EXTRACT(hour FROM wd.updated_time) as updated_time,
                round(avg(wd.temp_c) :: numeric,2) as temp
            FROM student.ss_weather_detail wd
                JOIN ss_weather_locations swl
                    ON wd.loc_key = swl.loc_key
            WHERE date_part('year', wd.updated_date) = date_part('year', CURRENT_DATE)
                and date_part('month', wd.updated_date) = date_part('month', CURRENT_DATE)
                and wd.loc_key = '{location.lower()}'
                and date_part('day', wd.updated_date) = {date}
            GROUP BY updated_time;
        ''')

    result = cursor.fetchall()
    df_temp = pd.DataFrame(result, columns=['updated_date', 'temp'])

    x = [int(i) for i in df_temp['updated_date']]
    y = df_temp['temp'].tolist()
    print(x)
    print(y)
    plt.plot(x, y, linewidth='4', marker='o')

else:
    cursor.execute(
        '''
            SELECT
                swl.loc_key,
                date_part('day', wd.updated_date) as updated_date,
                round(avg(wd.temp_c) :: numeric,2) as temp
            FROM student.ss_weather_detail wd
                JOIN ss_weather_locations swl
                    ON wd.loc_key = swl.loc_key
            WHERE date_part('year', wd.updated_date) = date_part('year', CURRENT_DATE)
                and date_part('month', wd.updated_date) = date_part('month', CURRENT_DATE)
            GROUP BY swl.loc_key,updated_date;
    ''')

    #%%
    result = cursor.fetchall()
    df_temp = pd.DataFrame(result, columns=['city','updated_date', 'temp'])

# filtering data
    dict_x = {}
    dict_y = {}
    locations = ['London', 'Birmingham', 'Manchester', 'Glasgow', 'Sheffield',
             'Leeds', 'Bristol', 'Nottingham',  'Liverpool'
             ]

    for i in range(len(locations)):
        location = locations[i].lower()
        dict_x['x'+str(i+1)] = [int(i) for i in df_temp['updated_date'][~df_temp['updated_date'].where(df_temp.city.str.strip() == location).isna()]]
        dict_y['y'+str(i+1)] = df_temp['temp'][~df_temp['temp'].where(df_temp.city.str.strip() == location).isna()].tolist()
    print(dict_x)
    plt.plot(dict_x['x1'], dict_y['y1'],dict_x['x2'], dict_y['y2'],dict_x['x3'],
             dict_y['y3'],dict_x['x4'], dict_y['y4'], dict_x['x5'], dict_y['y5'],
             dict_x['x6'], dict_y['y6'],dict_x['x7'], dict_y['y7'],dict_x['x8'], dict_y['y8'],
             dict_x['x9'], dict_y['y9'], linewidth='4',marker='o'
             )

#matplotlib
plt.xlabel(txt_x_label)
plt.ylabel("Temperature °C")
plt.legend(locations)
plt.grid(color='black')
plt.title(txt_title_label)
plt.show()

#%%
