from ETL import ETL

locations = {
    'Bolszewo': 'https://www.accuweather.com/en/pl/bolszewo/269913/current-weather/269913',
    'Sopot': 'https://www.accuweather.com/en/pl/sopot/265571/current-weather/265571',
    'Kraków': 'https://www.accuweather.com/en/pl/krakow/274455/current-weather/274455'
}

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'

for location, url in locations.items():

    ETL(location_name=location,
        URL=url,
        user_agent=user_agent,
        database='weather.db')

