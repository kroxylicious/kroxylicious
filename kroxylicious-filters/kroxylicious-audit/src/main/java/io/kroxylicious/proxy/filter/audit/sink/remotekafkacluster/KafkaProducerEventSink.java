/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.audit.sink.remotekafkacluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.proxy.filter.audit.Event;
import io.kroxylicious.proxy.filter.audit.EventSink;
import io.kroxylicious.proxy.filter.audit.sink.remotekafkacluster.KafkaProducerEventSinkService.Config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.UUIDSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KafkaProducerEventSink implements EventSink {
    private final KafkaProducer<UUID, Event> producer;
    private final String topic;

    public KafkaProducerEventSink(Config configuration) {
        this.topic = configuration.topic();

        var copy = new HashMap<>(configuration.producerConfig());
        copy.computeIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, k -> UUIDSerializer.class.getName());
        copy.computeIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, k -> JsonEventSerializer.class.getName());
        this.producer = new KafkaProducer<>(copy);
    }

    @Override
    public void acceptEvents(@NonNull List<Event> events) {
        events.forEach(event -> producer.send(new ProducerRecord<>(topic, event.eventId(), event)));
    }


    public static class JsonEventSerializer implements Serializer<Event> {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override

        public byte[] serialize(String topic, Event data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            }
            catch (JsonProcessingException e) {
                throw new SerializationException("Error when serializing event", e);
            }
        }
    }
}
