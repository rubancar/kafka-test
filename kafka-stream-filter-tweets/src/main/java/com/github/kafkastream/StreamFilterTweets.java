package com.github.kafkastream;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build the topology
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> {
                    return extractUserFollowersInTweet(jsonTweet) > 1000;
                }
        );

        filteredStream.to("important_tweets");

        // start our streams applications
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();
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
        public User user;
    }

    private static Gson gson = new Gson();

    public static int extractUserFollowersInTweet(String tweetJson) {
        // gson parse
        Tweet tweet = gson.fromJson(tweetJson, Tweet.class);
        return tweet.user.followers_count;
    }
}
