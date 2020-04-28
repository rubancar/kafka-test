package kafka.elasticsearch;

import com.google.gson.Gson;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static Dotenv dotenv = Dotenv.load();
    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) {
        RestHighLevelClient client = createClient();

        // Types are in the process of being removed
        // See documentation about deprecated warning :https://www.javadoc.io/doc/org.elasticsearch/elasticsearch/7.0.0/org/elasticsearch/action/index/IndexRequest.html#%3Cinit%3E(java.lang.String,java.lang.String)
        // See documentation of current usage: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-index.html
        IndexRequest indexRequest = new IndexRequest("twitter");

        try {
            KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

            // poll for new data
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.x

                Integer recordCount = records.count();

                logger.info("Received " +  recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record: records) {
                    // 2 strategies to make the consumer idempotent
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // Twitter feed specific id
                    try {
                        String id = extractIdFromTweet(record.value());
                        // where we insert data into Elasticsearch
                        String jsonString = record.value();
                        indexRequest.id(id); // to make the consumer idempotent
                        indexRequest.source(jsonString, XContentType.JSON);
                        bulkRequest.add(indexRequest); // we add to our bulk request

                    } catch (NullPointerException e) {
                        logger.error("Error while processing tweet", record.value());
                        logger.error("NullPointerException", e);
                        recordCount--;
                    }

                }

                if(recordCount > 0) {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Committing offsets...");
                    consumer.commitSync();
                    logger.info("Offset have been commited");
                    Thread.sleep(1000);
                }
            }

            // close the client gracefully
            // client.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }


    }

    public class User {
        public String name = null;
        public int followers_count = 0;
    }

    public class Tweet {
        public String created_at = null;
        public String id_str = null;
        public transient int    id = 0; // exclude from serialization and deserialization
        public String text = null;
        User user;
    }

    private static Gson gson = new Gson();

    public static String extractIdFromTweet(String tweetJson) {
        // gson parse
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);
        // logger.info("Followers count: "+ Integer.toString(tweet.user.followers_count));
        return tweet.id_str;
    }

    public static RestHighLevelClient createClient() {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(dotenv.get("ELASTIC_USERNAME"), dotenv.get("ELASTIC_PASSWORD")));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(dotenv.get("ELASTIC_HOST"), 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder
                        .HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServer = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        // if we are saving in bulk to ElasticSearch it's possible
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // we should only received 10 records at time

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

}
