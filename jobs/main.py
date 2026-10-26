import os 
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
import simplejson as json
import random
import uuid
import time
import simplejson as json

LONNDON_COORDINATE = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATE = {"latitude": 52.4862, "longitude": -1.8904}


# CALCULATE INCREMENTS FOR LATITUDE AND LONGITUDE
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATE["latitude"] - LONNDON_COORDINATE["latitude"]) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATE["longitude"] - LONNDON_COORDINATE["longitude"]) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

random.seed(42)
start_time = datetime.now()
start_location = LONNDON_COORDINATE.copy()


def get_next_time():

    global start_time

    start_time += timedelta(seconds=random.randint(30, 60)) # update frequency

    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):

    return{

        'id': str(uuid.uuid4()),
        'deviceID': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type

    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):

    return{
        'id': str(uuid.uuid4()),
        'deviceID':device_id,
        'camera_id':camera_id,
        'location':location,
        'timestamp':timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_emergency_incidence_data(timestamp, device_id, location):

    return{
            'id': str(uuid.uuid4()),
            'deviceID':device_id,
            'incidentid':str(uuid.uuid4()),
            'type': random.choice(['Accident','Fire','Police','None']),
            'location':location,
            'timestamp':timestamp,
            'status': random.choice(['Active','Resloved']),
            'description': 'Description of incidence'
        
        }


def generate_weather_data(device_id, timestamp, location):

    return{
            'id': str(uuid.uuid4()),
            'DeviceID':device_id,
            'Location':location,
            'Timestamp':timestamp,
            'Temperature': random.uniform(-5,26),
            'WeatherCondition': random.choice(['sunny','cloudy','rain','snow']),
            'Precipiation': random.uniform(0, 25), 
            'Windspeed': random.uniform(0,100),
            'Humidity': random.randint(0, 100),
            'AirQualityIndex':random.uniform(0, 500)
        }


def simulate_vehicle_movement():

    global start_location

    #move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # add randamness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    return {
        'id': str(uuid.uuid4()),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'maker': 'BMW',
        'Model': 'c-class',
        'year': 2024,
       'fuel_type': 'hybrid'
    }



def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f' object of type {obj.__class__.__name__} is not json serialisable')


def delivery_report(err, msg):
    if err is not None:
        print(f'message delivery failed: {err}')
    else:
        print(f'message delivered to {msg.topic()} [{msg.partition()}]')




def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery = delivery_report
    )

    producer.flush()


def simulate_journey(producer, device_id):
    
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data (device_id, vehicle_data['timestamp'],vehicle_data['location'], camera_id='Nikon-Cam123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incidence_data = generate_emergency_incidence_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATE['latitude']
        and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATE['longitude']):
            print('vehicle has reached birmingham. simulation ending....')
            break
        
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incidence_data)


        #print(vehicle_data)
        #print(gps_data)
        #print(traffic_camera_data)
        #print(weather_data)
        #print(emergency_incidence_data)
        
        time.sleep(5)
    





if __name__ == "__main__":

    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'kafka error: {err}')

    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'vehicle-Olamze-jwny')
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'unexpected error occurred: {e}')

    


