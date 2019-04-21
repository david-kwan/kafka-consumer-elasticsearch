package com.github.davidkwan.kafka.elasticsearch;

import com.github.davidkwan.kafka.consumer.Consumer;
import com.github.davidkwan.kafka.helpers.JsonHelper;
import com.github.davidkwan.kafka.httpclient.HttpRestClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
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

                Integer recordCount = records.count();
                logger.info("Recieved " + recordCount + " records.");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {

                    // twitter feed specific id
                    String id = JsonHelper.getJsonPropertyValue(record.value(), "id_str");

                    // create idempotent request to prevent duplicate data
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id
                    ).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                }

                if(recordCount > 0) {

                    client.bulk(bulkRequest, RequestOptions.DEFAULT);

                    logger.info("Commiting offsets");
                    consumer.commitSync();
                    logger.info("Offsets committed.");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Response error.", e);
        } catch (NullPointerException e) {
            logger.warn("Bad data point.", e);
        }

    }

}
