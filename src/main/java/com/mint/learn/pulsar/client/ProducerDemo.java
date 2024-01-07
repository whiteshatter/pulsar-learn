package com.mint.learn.pulsar.client;

import com.mint.learn.pulsar.utils.PulsarClientUtil;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;

public class ProducerDemo {
    public static void main(String[] args) {
        ProducerDemo.sendMessage();
    }

    public static void sendMessage() {
        PulsarClient clinet;
        try {
            clinet = PulsarClient.builder()
                    .serviceUrl(PulsarClientUtil.PULSAR_SERVICE_URL)
                    .ioThreads(1)
                    .listenerThreads(1)
                    .build();

            Producer<byte[]> producer = clinet.newProducer()
                    .topic("demo_topic")
                    .create();
            producer.send("同步消息".getBytes());
            producer.sendAsync("异步消息".getBytes())
                    .thenAccept(msgId -> System.out.println("发送成功"))
                    .exceptionally(throwable -> {
                        System.out.println("发送失败");
                        return null;
                    });
            producer.close();
            clinet.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sendKeyMsg() throws PulsarClientException {
        PulsarClient client = PulsarClientUtil.getClient();
        Producer<KeyValue<String, String>> keyTopic =
                client.newProducer(Schema.KeyValue(Schema.STRING, Schema.STRING)).topic("key_topic").create();
        keyTopic.newMessage().value(new KeyValue<>("key", "key msg")).send();
    }
}
