package com.github.davidkwan.kafka.elasticsearch;

import com.github.davidkwan.kafka.consumer.Consumer;
import com.github.davidkwan.kafka.httpclient.HttpRestClient;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ElasticSearchConsumer {

    private  static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = new HttpRestClient().createRestClient();

        KafkaConsumer<String, String> consumer = new Consumer("twitter_tweets").createConsumer();

        try {
            // bonsai free tier indices have limit of 1000 entries
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {

                    // kafka generic id
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // twitter feed specific id
                    String id = getJsonPropertyValue(record.value(), "id_str");

                    // create idempotent request to prevent duplicate data
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id
                    ).source(record.value(), XContentType.JSON);

                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                    logger.info(indexResponse.getId());
                }
            }
        } catch (IOException e) {
            logger.error("Response error.", e);
        }

    }

    private static String getJsonPropertyValue(String json, String property) {
        return jsonParser.parse(json)
                .getAsJsonObject()
                .get(property)
                .getAsString();
    }

}
