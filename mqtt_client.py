#!/usr/bin/python

from __future__ import division
import paho.mqtt.client as mqtt
import json
import time
import datetime
import Queue
import threading
import ConfigParser
import MySQLdb 
from decimal import Decimal
import sys
import logging
import logging.config
import urllib2
from threading import Lock
import schedule
import warnings
from sqlalchemy import create_engine
from sqlalchemy import Column, String
from sqlalchemy import exc as exception
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import db_model
import signal
import os


# Boolean for main thread
main_thread_alive = True

# Some settings for catching MySQL warnings
warnings.filterwarnings('error', category=MySQLdb.Warning)

# Loading configuration file
config_file_name = 'server_example.conf'
data_queue = Queue.Queue()
config = ConfigParser.ConfigParser()
config.read(config_file_name)

# Setting up logger
file_name, extension = os.path.splitext(os.path.basename(sys.argv[0]))
logger = logging.getLogger(file_name)
handler = logging.handlers.RotatingFileHandler(('/var/log/mqttClientsLog/' + file_name + '.log'), maxBytes=10485670, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Database name and table for making queries
db_name = config.get('Database', 'database')
db_table = config.get('Database', 'table')

# Number of threads for handling MQTT messages
worker_threads = 1

# Topic for publish and subscription
subs_topic = config.get("MqttBroker", "topic")

# AppEUI for filtering messages from P2PSmartTest sensors only
app_eui = "e7-19-b9-f4-a8-92-60-25"

# MQTT Broker URL
broker_url = config.get("MqttBroker", "url")

# MQTT Broker Port
broker_port = config.get("MqttBroker", "port")

# URL for weather updates
weather_url = config.get("WeatherAPI", "url")

city = "a"
weatherCon = "b"
temperature = 0
pressure = 0
humidity = 0
windSpeed = 0
timestampWeather = 0

lock = Lock()


# Handler for Kill signal
def terminate_signal_handler(signal, frame):
    logger.critical('Got Termination signal from system')
    global main_thread_alive
    main_thread_alive = False
    disconnect_mqtt_client()
    logging.shutdown()
    sys.exit(1)


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

    except urllib2.HTTPError as e:
        logger.error("HTTP error occured while accessing weather API. Error: " + e.code)
    except KeyboardInterrupt:
        db.close()
        logger.critical("Database connection closed.")
        logger.critical("System interrupted, exiting system!")
        sys.exit(1)


# This is asynchronous thread for inserting data to MySQL database
class insert_thread(threading.Thread):

    def __init__(self, queue, db_conn_str):
        threading.Thread.__init__(self)
        self.queue = queue
        self.initiate_db_session(db_conn_str)


    def initiate_db_session(self, db_conx_str):
        self.db = create_engine(db_conx_str, pool_recycle=15)
        self.Session = sessionmaker(self.db)
        #self.session = self.Session()
        logger.info("Connected to database.")


    def terminate_db_session(self):
        self.session.rollback()
        self.session.close()


    def run(self):
        global main_thread_alive
        while True:
            if main_thread_alive:
                # Running pending scheduler tasks
                schedule.run_pending()

                db_mesg = self.parse_message(self.queue.get())
                if db_mesg:
                    #print(db_mesg)
                    if db_mesg['AppEUI'] == app_eui:
                        new_record = db_model.SensorDatum(
                                ACK = db_mesg['ACK'],
                                ADR = db_mesg['ADR'],
                                AppEUI = db_mesg['AppEUI'],
                                CHAN = db_mesg['CHAN'],
                                CLS = db_mesg['CLS'],
                                RAD_CODR = db_mesg['CODR'],
                                DeviceID = db_mesg['DeviceID'],
                                RAD_FREQ = db_mesg['FREQ'],
                                RAD_LSNR = db_mesg['LSNR'],
                                MHDR = db_mesg['MHDR'],
                                MODU = db_mesg['MODU'],
                                OPTS = db_mesg['OPTS'],
                                PORT = db_mesg['PORT'],
                                RFCH = db_mesg['RFCH'],
                                RAD_RSSI = db_mesg['RSSI'],
                                RAD_SEQN = db_mesg['SEQN'],
                                Size = db_mesg['Size'],
                                timestamp_node = db_mesg['timestamp_node'],
                                Payload = db_mesg['Payload'],
                                MsgID = db_mesg['MsgID'],
                                V1 = db_mesg['V1'],
                                A1 = db_mesg['A1'],
                                V2 = db_mesg['V2'],
                                A2 = db_mesg['A2'],
                                V3 = db_mesg['V3'],
                                A3 = db_mesg['A3'],
                                kWIII = db_mesg['kWIII'],
                                kvarLIII = db_mesg['kvarLIII'],
                                PFLIII = db_mesg['PFLIII'],
                                WeatherCon = db_mesg['WeatherCon'],
                                Temperature = db_mesg['Temperature'],
                                Pressure = db_mesg['Pressure'],
                                Humidity = db_mesg['Humidity'],
                                City = db_mesg['City'],
                                WindSpeed = db_mesg['WindSpeed'],
                                TimestampWeather = db_mesg['TimestampWeather']
                                )
                        try:
                            #self.Session = sessionmaker(self.db)
                            self.session = self.Session()
                            #print("Session initiated")
                            self.session.add(new_record)
                            self.session.commit()
                            #print("Data has been successfully added.")
                        except exception.DBAPIError as e:
                            if e.connection_invalidated:
                                logger.error('Connection was invalidated. Not panicking, will try to reconnect to the database.')
                                global db_conn_string
                                self.initiate_db_session(db_conn_string)
                        except exception.SQLAlchemyError as e:
                            logger.error("Error occured while executing MySQL query.")
                            logger.error("Error: " + e.message)
                            self.session.rollback()
                        finally:
                            self.session.close()
                            #print("Session is closed")
                    self.queue.task_done()
            else:
                self.terminate_db_session()
                sys.exit(1)


    # This messages parser is specifically for P2PSmartTest sensors only.
    # For other sensors, you can write your own parser.
    def parse_message(self, raw_mesg):
        db_mesg_json = {}
        payload = []
        try:
            # raw_mesg_json is the message obtained from broker
            raw_mesg_json = json.loads(raw_mesg)
            #print(raw_mesg_json)
            # db_mesg_json is JSON object with respect to database.
            # This is kind of mapping raw_mesg_json to db_mesg_json
            db_mesg_json["ACK"] = raw_mesg_json["ack"]
            db_mesg_json["ADR"] = raw_mesg_json["adr"]
            db_mesg_json["AppEUI"] = raw_mesg_json["appeui"]
            db_mesg_json["CHAN"] = raw_mesg_json["chan"]
            db_mesg_json["CLS"] = raw_mesg_json["cls"]
            db_mesg_json["CODR"] = raw_mesg_json["codr"]
            db_mesg_json["DeviceID"] = raw_mesg_json["deveui"]
            db_mesg_json["FREQ"] = Decimal(raw_mesg_json["freq"])
            db_mesg_json["LSNR"] = Decimal(raw_mesg_json["lsnr"])
            db_mesg_json["MHDR"] = raw_mesg_json["mhdr"]
            db_mesg_json["MODU"] = raw_mesg_json["modu"]
            if not raw_mesg_json["opts"]:
                db_mesg_json["OPTS"] = None
            else:
                db_mesg_json["OPTS"] = raw_mesg_json["opts"]
            db_mesg_json["PORT"] = raw_mesg_json["port"]
            db_mesg_json["RFCH"] = raw_mesg_json["rfch"]
            db_mesg_json["RSSI"] = raw_mesg_json["rssi"]
            db_mesg_json["SEQN"] = raw_mesg_json["seqn"]
            mesg_size = raw_mesg_json["size"]
            db_mesg_json["Size"] = mesg_size
            timestamp_str  = raw_mesg_json["timestamp"]
            # TODO Have to correct this. This does not produce correct result.
            db_mesg_json["timestamp_node"] = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple())
            payload = raw_mesg_json["payload"]
            db_mesg_json["Payload"] = str(payload)
            db_mesg_json["MsgID"] = raw_mesg_json["_msgid"]

            # V1
            db_mesg_json["V1"] = ((payload[35]*256*256*256) + (payload[34]*256*256) + (payload[33]*256) + payload[32])/10

            # A1
            a1_temp = (payload[31]*256*256*256) + (payload[30]*256*256) + (payload[29]*256) + payload[28]
            if a1_temp < 2147483648:
                db_mesg_json["A1"] = a1_temp*50/1000
            else:
                db_mesg_json["A1"] = (a1_temp - 4294967296)*50/1000

            # v2
            db_mesg_json["V2"] = ((payload[27]*256*256*256) + (payload[26]*256*256) + (payload[25]*256) + payload[24])/10

            # A2
            a2_temp = (payload[23]*256*256*256) + (payload[22]*256*256) + (payload[21]*256) + payload[20]
            if a2_temp < 2147483648:
                db_mesg_json["A2"] = a2_temp*50/1000
            else:
                db_mesg_json["A2"] = (a2_temp - 4294967296)*50/1000
 
            # V3
            db_mesg_json["V3"] = ((payload[19]*256*256*256) + (payload[18]*256*256) + (payload[17]*256) + payload[16])/10

            # A3
            a3_temp = (payload[15]*256*256*256) + (payload[14]*256*256) + (payload[13]*256) + payload[12]
            if a3_temp < 2147483648:
                db_mesg_json["A3"] = a3_temp*50/1000
            else:
                db_mesg_json["A3"] = (a3_temp - 4294967296)*50/1000
 
            # kWIII
            kwiii_temp = (payload[11]*256*256*256) + (payload[10]*256*256) + (payload[9]*256) + payload[8]
            if kwiii_temp < 2147483648:
                db_mesg_json["kWIII"] = kwiii_temp*50/1000
            else:
                db_mesg_json["kWIII"] = (kwiii_temp - 4294967296)*50/1000
 
            # kvarLIII
            kvarliii_temp = (payload[3]*256*256*256) + (payload[2]*256*256) + (payload[2]*256) + payload[0]
            if kvarliii_temp < 2147483648:
                db_mesg_json["kvarLIII"] = kvarliii_temp
            else:
                db_mesg_json["kvarLIII"] = kvarliii_temp - 4294967296
  
            # PFLIII
            pfliii_temp = (payload[7]*256*256*256) + (payload[6]*256*256) + (payload[5]*256) + payload[4]
            if pfliii_temp < 2147483648:
                db_mesg_json["PFLIII"] = pfliii_temp
            else:
                db_mesg_json["PFLIII"] = pfliii_temp - 4294967296

            global city
            global weatherCon
            global temperature
            global pressure
            global humidity
            global windSpeed
            global timestampWeather 
            lock.acquire()
            db_mesg_json["WeatherCon"] = weatherCon
            db_mesg_json["Temperature"] = temperature
            db_mesg_json["Pressure"] = pressure
            db_mesg_json["Humidity"] = humidity
            db_mesg_json["City"] = city
            db_mesg_json["WindSpeed"] = windSpeed
            db_mesg_json["TimestampWeather"] = timestampWeather
            lock.release()
            
            return db_mesg_json

        except ValueError as e:
            logger.warning("Incomming message is not a valid JSON.")
            logger.warning("Error: " + e.message)
            logger.warning(raw_mesg)
            sys.stdout.write(e.message + '\n')
            sys.stdout.flush()
            return False

        except Exception as e:
            logger.warning("Something went wrong. Ignoring message.")
            logger.error("Error: " + e.message)
            sys.stdout.write(e.message + '\n')
            sys.stdout.flush()
            return False


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to the broker with code: " + str(rc))
        client.connected_flag = True

        # Topic for CWC CloudMQTT broker
        client.subscribe(subs_topic)

    else:
        logger.critical("Error occured while connecting to the broker. Error code: " + str(rc))


def on_message(client, userdata, mesg):
    data_queue.put(mesg.payload)
    #print("Got message from broker")


def on_disconnect(client, userdata, rc):
    logging.CRITICAL("Client disconnected. Trying to reconnect.")

def disconnect_mqtt_client():
    global mqtt_client
    mqtt_client.disconnect()
    mqtt_client.loop_stop()
    logger.critical('MQTT client has been disconnected.')

def init_client_object():

    client = mqtt.Client("mqttClient")

    broker_user = config.get('MqttBroker', 'user')
    broker_pass = config.get('MqttBroker', 'passwd')

    # Credentials for CloudMQTT broker
    client.username_pw_set(broker_user, password=broker_pass)
    client.connected_flag = False
    client.on_connect = on_connect
    client.on_message = on_message

    return client


def connect_client():

    global mqtt_client
    # Server and port for CloudMQTT broker
    rc = mqtt_client.connect(broker_url, port=broker_port, keepalive=60)

    # Server and port for Panoulu broker (localhost only)
    if rc == 0:
        mqtt_client.loop_forever()
    else:
        logger.critical("Connection to the broker failed. Return code: " + str(rc))


if __name__ == '__main__':
    
    # Setting handler for 'Terminate and ''Kill' signal by system
    signal.signal(signal.SIGTERM, terminate_signal_handler)

  
    # Getting weather updates
    get_weather_updates()
    schedule.every().hour.do(get_weather_updates)

    # Initializing database
    db_host = config.get('Database', 'host')
    db_port = config.get('Database', 'port')
    db_user = config.get('Database', 'user')
    db_pass = config.get('Database', 'passwd')
    db_name = config.get('Database', 'database')
    db_table = config.get('Database', 'table')    

    global db_conn_string
    db_conn_string = ('mysql://%s:%s@%s:%s/%s') % (db_user, db_pass, db_host, db_port, db_name)
    
    thread_record = []

    try:
        for i in range(worker_threads):
            t = insert_thread(data_queue, db_conn_string)
            t.setDaemon(True)
            t.start()
            thread_record.append(t)
            logger.info("%s has started", t.getName())

        # Initializing MQTT client and connecting it to localhost broker
        global mqtt_client
        mqtt_client = init_client_object()
        connect_client()

    except KeyboardInterrupt:
        logger.critical("Database connection closed.")
        logger.critical("System interrupted, exiting system!")
        logging.shutdown()
        sys.exit(1)
