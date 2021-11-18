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
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.debezium.kafka.KafkaCluster;
import io.strimzi.kproxy.interceptor.AdvertisedListenersInterceptor;
import io.strimzi.kproxy.interceptor.ApiVersionsInterceptor;
import io.strimzi.kproxy.interceptor.Interceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static java.lang.Integer.parseInt;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProxyST {

    @Test
    public void test1() throws Exception {
        String proxyHost = "localhost";
        int proxyPort = 9192;
        String proxyAddress = String.format("%s:%d", proxyHost, proxyPort);

        String brokerList = startKafkaCluster();

        var interceptors = List.of(
                new ApiVersionsInterceptor(),
                new AdvertisedListenersInterceptor(new AdvertisedListenersInterceptor.AddressMapping() {
                    @Override
                    public String host(String host, int port) {
                        return proxyHost;
                    }

                    @Override
                    public int port(String host, int port) {
                        return proxyPort;
                    }
        }));

        startProxy(proxyHost, proxyPort, brokerList, interceptors);

        // TODO Use the proxy addresss!
        var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        producer.send(new ProducerRecord<>("my-test-topic", "Hello, world!")).get();
        producer.close();

        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));
        consumer.subscribe(Set.of("my-test-topic"));
        var records = consumer.poll(Duration.ofSeconds(10));
        consumer.close();
        assertEquals(1, records.count());
        assertEquals("Hello, world!", records.iterator().next().value());

        // TODO shutdown the proxy
    }

    private void startProxy(String proxyHost, int proxyPort, String brokerList, List<Interceptor> interceptors) {

        String[] hostPort = brokerList.split(",")[0].split(":");

        var th = new Thread(() -> {
            try {
                KafkaProxy.run(proxyHost, proxyPort, hostPort[0], parseInt(hostPort[1]), false, false, interceptors);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        th.start();
    }

    private String startKafkaCluster() throws IOException {
        var kafkaCluster = new KafkaCluster()
                .addBrokers(1)
                .usingDirectory(Files.createTempDirectory(ProxyST.class.getName()).toFile())
                //.withKafkaConfiguration()
                .startup();

        var brokerList = kafkaCluster.brokerList();
        return brokerList;
    }

}
