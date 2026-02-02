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
  "name": "Supplier",
  "fields": [
    {"name": "supplierId", "type": "int"},
    {"name": "supplierName", "type": "string"},
    {"name": "address", "type": "string"},
    {"name": "BankAccount", "type": "string"},
    {"name": "rating", "type": "double"}
  ]
}
"""

#{"name": "salename", "type": "string", "default": "unknown"} # Experiment : Add field     

# 2. Setup Registry Client and Serializer
sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)
avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str)

# 3. Setup Producer
producer_conf = {
    'bootstrap.servers': 'localhost:8097,localhost:8098,localhost:8099',
    'key.serializer': StringSerializer(),
    'value.serializer': avro_serializer
}

p = SerializingProducer(producer_conf)
        
data = {"supplierId": 1, "supplierName": "A", "address": "1/1", "BankAccount" : "123-123-123", "rating": 9.9 }
print(data)
# Produce - SerializingProducer handles encoding automatically
p.produce(topic='supplier-topic-full', key="c001", value=data)
p.flush()
print("Avro message produced.")