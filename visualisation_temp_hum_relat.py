import matplotlib.pyplot as plt
import matplotlib
import psycopg2
import pandas as pd
from config import config
matplotlib.use('TkAgg')

conn_details = None
try:
    params = config()
    print('Connecting to the postgreSQL database ...')
    conn_details = psycopg2.connect(**params)
except(Exception, psycopg2.DatabaseError) as error:
    print(error)
conn_details.autocommit = True
cursor = conn_details.cursor()

locations = ['London', 'Birmingham', 'Manchester', 'Glasgow', 'Sheffield',
             'Leeds', 'Bristol', 'Nottingham',  'Liverpool'
             ]
location = input(f'''
                    What city from 
                    
                    {locations} 
                    
                    do you wish to discover about the monthly trend in daily temperature?
                    ''').title()
if location in locations:
    cursor.execute(
        f'''
            SELECT
                wd.humidity as humidity,
                wd.feelslike_c as feel
            FROM student.ss_weather_detail wd
            WHERE wd.loc_key = '{location.lower()}'
        ''')

    result = cursor.fetchall()
    df_temp = pd.DataFrame(result, columns=['humidity', 'feel'])

    x = df_temp['humidity'].tolist()
    y = df_temp['feel'].tolist()
    plt.xlabel('Humidity')
    plt.ylabel('Feel like temperature')
    plt.title(location)


    plt.scatter(x, y)
    plt.show()


#%%
