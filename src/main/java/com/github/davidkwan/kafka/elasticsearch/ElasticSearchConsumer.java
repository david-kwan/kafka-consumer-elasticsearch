package com.github.davidkwan.kafka.elasticsearch;

import com.github.davidkwan.kafka.consumer.Consumer;
import com.github.davidkwan.kafka.httpclient.HttpRestClient;
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

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = new HttpRestClient().createRestClient();

        KafkaConsumer<String, String> consumer = new Consumer("twitter_tweets").createConsumer();

        try {
            // bonsai free tier indices have limit of 1000 entries
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    record.value();

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets"
                    ).source(record.value(), XContentType.JSON);

                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                    String id = indexResponse.getId();
                    logger.info(id);
                }
            }
        } catch (IOException e) {
            logger.error("Response error.", e);
        }

    }
}
