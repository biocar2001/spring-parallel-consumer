Ejemplo de uso de parallel consumer confluent, consumiendo y produciendo con spring:

# DEMO:
- kafka-topics --create --topic parallel-consumer-input-topic --bootstrap-server broker:9092 --replication-factor 1 --partitions 1
- kafka-topics --create --topic parallel-consumer-ouput-topic --bootstrap-server broker:9092 --replication-factor 1 --partitions 1


- kafka-console-producer --topic parallel-consumer-input-topic --bootstrap-server broker:9092 --property "parse.key=true" --property "key.separator=:"

`fun-line:All streams lead to Kafka
event-promo:Go to Current
event-promo:Go to Kafka Summit
fun-line:Consume gently down the stream`



- kafka-console-consumer --topic parallel-consumer-output-topic --bootstrap-server broker:9092 --property "parse.key=true" --property "key.separator=:"

