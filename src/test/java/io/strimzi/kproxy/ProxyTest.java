/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import io.debezium.kafka.KafkaCluster;
import io.strimzi.kproxy.internal.FilterChainFactory;
import io.strimzi.kproxy.internal.interceptor.AdvertisedListenersInterceptor;
import io.strimzi.kproxy.internal.interceptor.AdvertisedListenersInterceptor.AddressMapping;
import io.strimzi.kproxy.internal.interceptor.ApiVersionsInterceptor;
import io.strimzi.kproxy.internal.interceptor.ProduceRecordTransformationInterceptor;
import io.strimzi.kproxy.util.SystemTest;

import static java.lang.Integer.parseInt;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SystemTest
public class ProxyTest {

    @Test
    public void shouldPassThroughRecordUnchanged() throws Exception {
        String proxyHost = "localhost";
        int proxyPort = 9192;
        String proxyAddress = String.format("%s:%d", proxyHost, proxyPort);

        String brokerList = startKafkaCluster();

        FilterChainFactory filterChainFactory = () -> List.of(
                new ApiVersionsInterceptor(),
                new AdvertisedListenersInterceptor(new FixedAddressMapping(proxyHost, proxyPort)));

        var proxy = startProxy(proxyHost, proxyPort, brokerList, filterChainFactory);

        var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        producer.send(new ProducerRecord<>("my-test-topic", "my-key", "Hello, world!")).get();
        producer.close();

        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        consumer.subscribe(Set.of("my-test-topic"));
        var records = consumer.poll(Duration.ofSeconds(10));
        consumer.close();
        assertEquals(1, records.count());
        assertEquals("Hello, world!", records.iterator().next().value());

        // shutdown the proxy
        proxy.shutdown();
    }

    @Test
    public void shouldModifyProduceMessage() throws Exception {
        String proxyHost = "localhost";
        int proxyPort = 9192;
        String proxyAddress = String.format("%s:%d", proxyHost, proxyPort);

        String brokerList = startKafkaCluster();

        FilterChainFactory filterChainFactory = () -> List.of(
                new ApiVersionsInterceptor(),
                new AdvertisedListenersInterceptor(new FixedAddressMapping(proxyHost, proxyPort)),
                new ProduceRecordTransformationInterceptor(
                        buffer -> ByteBuffer.wrap(new String(StandardCharsets.UTF_8.decode(buffer).array()).toUpperCase().getBytes(StandardCharsets.UTF_8))));

        var proxy = startProxy(proxyHost, proxyPort, brokerList,
                filterChainFactory);

        var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_600_000));
        producer.send(new ProducerRecord<>("my-test-topic", "my-key", "Hello, world!")).get();
        producer.close();

        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        consumer.subscribe(Set.of("my-test-topic"));
        var records = consumer.poll(Duration.ofSeconds(10));
        consumer.close();
        assertEquals(1, records.count());
        assertEquals("HELLO, WORLD!", records.iterator().next().value());

        // shutdown the proxy
        proxy.shutdown();
    }

    private KafkaProxy startProxy(String proxyHost,
                                  int proxyPort,
                                  String brokerList,
                                  FilterChainFactory filterChainFactory)
            throws InterruptedException {
        String[] hostPort = brokerList.split(",")[0].split(":");

        KafkaProxy kafkaProxy = new KafkaProxy(proxyHost,
                proxyPort,
                hostPort[0],
                parseInt(hostPort[1]),
                true,
                true,
                filterChainFactory);
        kafkaProxy.startup();
        return kafkaProxy;
    }

    private String startKafkaCluster() throws IOException {
        var kafkaCluster = new KafkaCluster()
                .addBrokers(1)
                .usingDirectory(Files.createTempDirectory(ProxyTest.class.getName()).toFile())
                // .withKafkaConfiguration()
                .startup();

        return kafkaCluster.brokerList();
    }

    private static class FixedAddressMapping implements AddressMapping {

        private final String targetHost;
        private final int targetPort;

        public FixedAddressMapping(String targetHost, int targetPort) {
            this.targetHost = targetHost;
            this.targetPort = targetPort;
        }

        @Override
        public String host(String host, int port) {
            return targetHost;
        }

        @Override
        public int port(String host, int port) {
            return targetPort;
        }
    }
}
