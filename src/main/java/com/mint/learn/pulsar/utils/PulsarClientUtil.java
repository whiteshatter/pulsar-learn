package com.mint.learn.pulsar.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarClientUtil {
    public static String PULSAR_SERVICE_URL = "pulsar://47.99.81.54:6650";
    public static PulsarClient getClient() {
        try {
            return PulsarClient.builder()
                    .serviceUrl(PULSAR_SERVICE_URL)
                    .ioThreads(1)
                    .listenerThreads(1)
                    .build();
        } catch (PulsarClientException e) {
            log.info("获取pulsar客户端失败", e);
            throw new RuntimeException(e);
        }
    }

    public static Producer<byte[]> getBatchProducer() throws PulsarClientException {
        PulsarClient client = getClient();
        return client.newProducer()
                .topic("batch-topic")
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.SECONDS)
                .enableBatching(true)
                .create();
    }

    public static Producer<byte[]> getChunckProducer() throws PulsarClientException {
        PulsarClient client = getClient();
        return client.newProducer(Schema.BYTES)
                .topic("chunk-topic")
                .enableChunking(true)
                .enableBatching(false)
                .create();
    }
}
