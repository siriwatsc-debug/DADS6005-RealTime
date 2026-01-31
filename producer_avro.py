from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer

# 1. Define the Avro Schema
avro_schema_str = """
{
  "namespace": "example.avro",
  "type": "record",
  "name": "Movie",
  "fields": [
    {"name": "movieId", "type": "int"},
    {"name": "title", "type": "string"},
    {"name": "genres", "type": "string"},
    {"name": "rating", "type": "double"}
  ]
}
"""

# 2. Setup Registry Client and Serializer
sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

#======
# Setup Client
subject = 'supplier-avro-topic-value'

# PULL SCHEMA VERSION 1 FROM REGISTRY
# Note: This assumes you have already registered at least two versions
db_meta = schema_registry_client.get_version(subject, version=1)
avro_schema_str = db_meta.schema.schema_str

print(f"Producer pulled Schema Version: {db_meta.version}")
print(avro_schema_str)

#exit()
# Setup Producer
#producer = Producer({'bootstrap.servers': 'localhost:9092'})
#avro_serializer = AvroSerializer(sr_client, v1_schema_str)
#string_serializer = StringSerializer('utf_8')
#======

avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)


# 3. Setup Producer
producer_conf = {
    'bootstrap.servers': 'localhost:8097,localhost:8098,localhost:8099',
    'key.serializer': StringSerializer(),
    'value.serializer': avro_serializer
}

p = SerializingProducer(producer_conf)
        
#data = {"movieId": 1, "title": "Inception1", "genres": "Action, Sci-Fi", "rating": 1.9, "like" : 5}
data = {"supplierId": 2, "supplierName": "B", "address": "1/1", "BankAccount" : "123-123-123", "rating": 9.9 }
print(data)
# Produce - SerializingProducer handles encoding automatically
p.produce(topic='supplier-avro-topic', key="c001", value=data)
p.flush()
print("Avro message produced.")