package com.mint.learn.pulsar.Config;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.StringSchema;

import java.nio.ByteBuffer;

public class UpperConsumerInterceptor implements ProducerInterceptor {
    @Override
    public void close() {

    }

    @Override
    public boolean eligible(Message message) {
        if (message instanceof MessageImpl) {
            return (((MessageImpl) message).getSchema()) instanceof StringSchema;
        }
        return false;
    }

    @Override
    public Message beforeSend(Producer producer, Message message) {
        byte[] rawData = new String(message.getData()).toUpperCase().getBytes();
        return MessageImpl.create(((MessageImpl<?>) message).getMessageBuilder(), ByteBuffer.wrap(rawData), StringSchema.utf8());
    }

    @Override
    public void onSendAcknowledgement(Producer producer, Message message, MessageId messageId, Throwable throwable) {

    }
}
