import matplotlib
import assign_loc as l
import matplotlib.pyplot as plt
import psycopg2
from config import config
import pandas as pd
matplotlib.use('TkAgg')




location = input(f'''
                    What city from 
                    
                    {l.locations} 
                    
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

if location in l.locations:
    cursor.execute(
        f'''
            SELECT condition,count(condition) as c_count
            FROM student.ss_weather_detail 
            WHERE loc_key = '{location.lower()}'
                and date_part('year', updated_date) = date_part('year', CURRENT_DATE)
                and date_part('month', updated_date) = date_part('month', CURRENT_DATE)
            GROUP BY condition;

    ''')

    result = cursor.fetchall()
    df_temp = pd.DataFrame(result, columns=['condition', 'c_count'])
    condition_dic = df_temp.set_index('condition').to_dict()
    new_condition_dic = {}
    raincount= 0
    print(condition_dic)

    for i in condition_dic['c_count']:
        if 'rain' in i.lower():
            raincount += condition_dic['c_count'][i]
        else:
            new_condition_dic[i] = condition_dic['c_count'][i]
    new_condition_dic['rain'] = raincount
    print(new_condition_dic)

    y = new_condition_dic.values()
    mylabels = new_condition_dic.keys()

    plt.pie(y, labels = mylabels, startangle = 90)
    plt.title(location)
    plt.show()



