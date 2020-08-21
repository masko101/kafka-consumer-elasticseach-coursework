package com.markt.kafka.tutorial3;

import com.google.gson.JsonParser;
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
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static final String KAFKA_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer();

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(5000));

            BulkRequest bulkRequest = new BulkRequest();
            log.info("Recieved " + consumerRecords.count() +" records");
            for (ConsumerRecord<String, String> record : consumerRecords) {

//                String id = record.topic() + "_" + record.partition() + "_" + record.partition();
                try {
                    String twitterId = extractTwitterIdFromRecord(record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter").id(twitterId)
                            .source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                    //                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    //                String id = indexResponse.getId();
                    //                log.info("Id: " + id + " - twitterId: " + twitterId);

                    //                log.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                    log.info(String.format("Partition: %d, Offsets: %d", record.partition(), record.offset()));
                } catch (NullPointerException e) {
                    log.warn("Erk Bad Data: " + record.value());
                }
            }
            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Committing offsets...");
                consumer.commitSync();
                log.info("Offsets committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

//        client.close();
    }

    private static String extractTwitterIdFromRecord(String tweet) {
        return JsonParser.parseString(tweet).getAsJsonObject().get("id_str").getAsString();
    }

    private static KafkaConsumer<String, String> createConsumer() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("twit_twee_topic"));
        return consumer;
    }

    private static RestHighLevelClient createClient() {
        String hostname = "twit-search-test-7558362604.us-west-2.bonsaisearch.net";
        String key = "m01gdrjwqz";
        String secret = "kdznt0rayh";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(key, secret));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        return new RestHighLevelClient(builder);
    }
}
