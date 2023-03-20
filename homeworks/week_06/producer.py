import csv
import gzip

from confluent_kafka import Producer

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC, SOURCE_FILES


def delivery_callback(err, msg):
    if err is None:
        print(f"Message produced: {msg.key().decode('utf-8')}")
    else:
        print(f"Failed to deliver a message: {msg}: {err}")
        
        
def produce(file_path, config):
    with gzip.open(file_path, "rt") as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        
        producer = Producer(config)

        for row in csv_reader:
            location_id = row[3]
            if location_id:
                producer.poll(0)   
                producer.produce(
                    topic=KAFKA_TOPIC, 
                    key=str(location_id).encode(), 
                    value=str(1).encode(), 
                    callback=delivery_callback)
                producer.flush()  


if __name__ == "__main__":
    source_files = SOURCE_FILES
    config = {"bootstrap.servers": BOOTSTRAP_SERVERS}
    
    for source_file in source_files:
        produce(source_file, config)
               


