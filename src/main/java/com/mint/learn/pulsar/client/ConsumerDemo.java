package com.mint.learn.pulsar.client;

import com.mint.learn.pulsar.utils.PulsarClientUtil;
import org.apache.pulsar.client.api.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class ConsumerDemo {
    public static void main(String[] args) {
        receive();
    }

    public static Consumer<String> getConsumer() {
        PulsarClient client = PulsarClientUtil.getClient();
        try {
            return client.newConsumer(Schema.STRING)
                    .topic("demo_topic")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("test_sub")
                    .subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static void receive() {
        Consumer<String> consumer = getConsumer();
        try {
            Message<String> msg = consumer.receive();
            System.out.println("收到消息：" + new String(msg.getData()));
            consumer.acknowledge(msg);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

    }

    public static void asyncConsume() {
        Consumer<String> consumer = getConsumer();
        consumer.receiveAsync()
                .thenAccept((Message<String> msg) -> {
                    System.out.println("receive:" + Arrays.toString(msg.getData()));
                    try {
                        consumer.acknowledge(msg);
                    } catch (PulsarClientException e) {
                        consumer.negativeAcknowledge(msg);
                    }
                });
    }

    public static void batchConsume() throws PulsarClientException {
        Consumer<String> consumer = getConsumer();
        Messages<String> messages = consumer.batchReceive();
        for (Message<String> message : messages) {
            System.out.println("receive:" + Arrays.toString(message.getData()));
            // 单消息确认
            // try {
            //     consumer.acknowledge(message);
            // } catch (PulsarClientException e) {
            //     consumer.negativeAcknowledge(message);
            // }
        }
        // 批量确认
        consumer.acknowledge(messages);
    }

    public static Consumer<String> getGroupAckConsumer() {
        PulsarClient client = PulsarClientUtil.getClient();
        try {
            return client.newConsumer(Schema.STRING)
                    .topic("demo_topic")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("group_sub")
                    .acknowledgmentGroupTime(100, TimeUnit.MICROSECONDS)
                    .subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static void timeoutConsumer() {
        PulsarClient client = PulsarClientUtil.getClient();
        try {
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic("demo_topic")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("timeout_sub")
                    .ackTimeout(5, TimeUnit.SECONDS)
                    .acknowledgmentGroupTime(100, TimeUnit.MICROSECONDS)
                    .subscribe();
            while (true) {
                Message<String> msg = consumer.receive();
                System.out.println(msg.getData());
                consumer.acknowledge(msg);
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static void retryConsumer() {
        PulsarClient client = PulsarClientUtil.getClient();
        try {
            Consumer<String> consumer = client.newConsumer(Schema.STRING)
                    .topic("demo_topic")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("retry_sub")
                    .enableRetry(true)
                    .subscribe();
            while (true) {
                Message<String> msg = consumer.receive();
                System.out.println(msg.getData());
                consumer.reconsumeLater(msg, 10, TimeUnit.SECONDS);
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

    }

    public static void reader() {
        PulsarClient client = PulsarClientUtil.getClient();
        try {
            Reader<String> reader = client.newReader(Schema.STRING)
                    .topic("demo_topic")
                    .startMessageId(MessageId.earliest)
                    .create();
            while (true) {
                Message<String> msg = reader.readNext();
                System.out.println(msg.getData());
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
