import sqlite3
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pytz


def ETL(location_name: str, URL: str, user_agent: str, database: str):

    page = requests.get(URL, headers={'User-Agent': user_agent}).content
    soup = BeautifulSoup(page, 'html.parser')

    current_weather = soup.find('div', {'class': 'current-weather-card card-module content-module non-ad'})

    data = dict()

    data['timestamp'] = datetime.now(pytz.timezone('Europe/Warsaw'))

    real_feel_raw = (
        current_weather
            .find('div', {'class': 'current-weather-extra'})
            .text
            .replace('\t', '')
            .split('\n')
    )
    real_feel = [i for i in real_feel_raw if len(i) > 0]

    for i in real_feel:
        if i[:9] == 'RealFeel®':
            data['real_feel'] = i.split()[1][:-1]
        if i[:15] == 'RealFeel Shade™':
            data['real_feel_shade'] = i.split()[-1][:-1]

    data['temp'], data['temp_unit'] = (
        current_weather
            .find('div', {'class': 'temp'})
            .text
            .strip()
            .split('°')
    )

    data['phrase'] = (
        current_weather
            .find('div', {'class': 'phrase'})
            .text
    )

    stats_left = (
        current_weather
            .find('div', {'class': 'left'})
            .findAll('div', {'class': 'detail-item spaced-content'})
    )

    stats_right = (
        current_weather
            .find('div', {'class': 'right'})
            .findAll('div', {'class': 'detail-item spaced-content'})
    )

    for i in stats_left:
        x = i.findAll('div')
        data[x[0].text] = x[1].text

    for i in stats_right:
        x = i.findAll('div')
        data[x[0].text] = x[1].text

    if len(data['Wind'].split(maxsplit=2)) == 2:
        data['wind_speed'], data['wind_speed_unit'] = data['Wind'].split(maxsplit=2)
    else:
        data['wind_direction'], data['wind_speed'], data['wind_speed_unit'] = data['Wind'].split(maxsplit=2)
    del data['Wind']

    data['wind_gusts_speed'], data['wind_gusts_speed_unit'] = data['Wind Gusts'].split(maxsplit=1)
    del data['Wind Gusts']

    data['humidity_pct'] = data['Humidity'][:-1]
    del data['Humidity']

    data['cloud_cover_pct'] = data['Cloud Cover'][:-1]
    del data['Cloud Cover']

    data['indoor_humidity_pct'], data['indoor_humidity_desc'] = data['Indoor Humidity'].split(maxsplit=1)
    data['indoor_humidity_pct'] = data['indoor_humidity_pct'][:-1]
    del data['Indoor Humidity']

    data['dew_point'], data['dew_point_unit'] = data['Dew Point'].split('°', maxsplit=1)
    del data['Dew Point']

    data['pressure_sign'], data['pressure'], data['pressure_unit'] = data['Pressure'].split(maxsplit=2)
    del data['Pressure']

    data['visibility'], data['visibility_unit'] = data['Visibility'].split()
    del data['Visibility']

    data['cloud_ceiling'], data['cloud_ceiling_unit'] = data['Cloud Ceiling'].split()
    del data['Cloud Ceiling']

    if data.get('Max UV Index'):
        data['max_uv_index'], data['max_uv_index_desc'] = data['Max UV Index'].split(maxsplit=1)
        del data['Max UV Index']

    data['location'] = location_name

    df = pd.DataFrame(data, index=[0])

    con = sqlite3.connect(database)
    cursor = con.cursor()

    with open('create_table.sql', 'r') as f:
        create_table_query = f.read()

    cursor.execute(create_table_query)

    df.to_sql('weather', con, index=False, if_exists='append')

    con.close()
