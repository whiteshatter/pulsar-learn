package com.mint.learn.pulsar.client;

import com.mint.learn.pulsar.entity.DemoData;
import com.mint.learn.pulsar.entity.ProtocolData;
import com.mint.learn.pulsar.utils.PulsarClientUtil;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

public class SchemaDemo {
    public static void main(String[] args) throws PulsarClientException {
        Schema<KeyValue<Integer, String>> inlineSchema = Schema.KeyValue(Schema.INT32, Schema.STRING, KeyValueEncodingType.INLINE);
        Schema<KeyValue<Integer, String>> schema = Schema.KeyValue(Schema.INT32, Schema.STRING);
        PulsarClient client = PulsarClientUtil.getClient();
        Producer<DemoData> producer1 = client.newProducer(JSONSchema.of(DemoData.class)).create();
        Producer<DemoData> producer2 = client.newProducer(AvroSchema.of(DemoData.class)).create();

        // ProtobufSchema使用的对象需要继承GeneratedMessageV3
        Producer<ProtocolData> producer3 = client.newProducer(ProtobufSchema.of(ProtocolData.class)).create();
        Producer<ProtocolData> producer4 = client.newProducer(ProtobufNativeSchema.of(ProtocolData.class)).create();

    }
}
