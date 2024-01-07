package com.mint.learn.pulsar.Config;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

import java.util.Random;

public class RandomRouter implements MessageRouter {
    static Random random = new Random();
    @Override
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        int numPartitions = metadata.numPartitions();
        return random.nextInt(numPartitions);
    }
}
