# Kafka Test

This repo contains several examples using Kafka API, it includes Consumers, 
Consumers groups, Consumer replays and so on also includes various examples 
using Kafka Producers. Further an example of a real world Kafka usage is 
implemented using Twitter Streaming API.

### Prerequisites

This uses Kafka API and Twitter API.

### Twitter Kafka connect
Kafka connector version 0.0.26 from [this repo](https://github.com/jcustenborder/kafka-connect-twitter/releases/tag/0.2.26), we need to configure the file twitter.properties and then 
execute the following command:\
**connect-standalone.sh connect-standalone.properties twitter.properties**

### Built With

* [Maven](https://maven.apache.org/) - Dependency Management

