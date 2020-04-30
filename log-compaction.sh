 # create a topic with log compaction enabled
 kafka-topics.sh --bootstrap-server localhost:9092 --create --topic employee-salary --partitions 1
 --replication-factor 1 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.001 --config segment.ms=5000

 # verify configuration of the new topic
 kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic employee-salary

 #create a consumer with a special option to print the key separated by comma
 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic employee-salary --from-beginning --property print.key=true --property key.separator=,

 # create a kafka producer, also with a special option to produce to employee topics with a key

