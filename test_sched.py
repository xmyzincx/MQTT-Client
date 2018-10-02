import schedule
import urllib2
import json
from threading import Lock
import time
import datetime

city = "a"
weatherCon = "b"
temperature = 0
pressure = 0
humidity = 0
windSpeed = 0
timestampWeather = 0

weather_url = "http://api.openweathermap.org/data/2.5/weather?id=3120513&APPID=e82533a082ff49d72a97eac53f3f343e"

lock = Lock()

def get_weather_updates():
    try:
        response = urllib2.urlopen(weather_url)
        weather_data = json.loads(response.read())

        global city
        global weatherCon
        global temperature
        global pressure
        global humidity
        global windSpeed
        global timestampWeather
        lock.acquire()
        city = weather_data['name']
        weatherCon = weather_data['weather'][0]['description']
        humidity = weather_data['main']['humidity']
        pressure = weather_data['main']['pressure']
        temperature = weather_data['main']['temp'] - 273.15
        windSpeed = weather_data['wind']['speed']
        timestampWeather = int(round(time.time() * 1000))
        lock.release()

        print timestampWeather
    except urllib2.HTTPError as e:
        logger.error("HTTP error occured while accessing weather API. Error: " + e.code)
    except KeyboardInterrupt:
        db.close()
        logger.critical("Database connection closed.")
        logger.critical("System interrupted, exiting system!")
        sys.exit(1)




if __name__ == '__main__':
    get_weather_updates()
    schedule.every().minute.do(get_weather_updates)

    while True:
        schedule.run_pending()
        time.sleep(3)
