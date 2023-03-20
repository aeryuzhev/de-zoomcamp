from collections import defaultdict

from confluent_kafka import Consumer

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC


def get_popular_locations(locations_dict: dict, top: int = 3):
    sorted_locations = sorted(
        locations_dict.items(), 
        key=lambda item: item[1], 
        reverse=True
    )
    return sorted_locations[:top]


def consume(config):
    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC])
    
    locations_dict = defaultdict(int)
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue                
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                location_id = msg.key().decode("utf-8")
                locations_dict[location_id] += 1
                
                print(get_popular_locations(locations_dict))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()   


if __name__ == "__main__":
    config={
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "locations",
        "auto.offset.reset": "earliest",
    }
    
    consume(config)


