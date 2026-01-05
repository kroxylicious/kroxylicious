/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.base.Stopwatch;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.simpletransform.NewCachingProduceRequestTransformation;
import io.kroxylicious.proxy.filter.simpletransform.NewProduceRequestTransformation;
import io.kroxylicious.proxy.filter.simpletransform.UpperCasing;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.record.RecordTestUtils;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.test.tester.KroxyliciousTesters;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class MetadataTimingIT {

    public static final int REPETITIONS = 25000;

    public static Stream<Arguments> measureLatency() {
        return Stream.of(Arguments.of(NewProduceRequestTransformation.class.getName(), 0),
                Arguments.of(NewProduceRequestTransformation.class.getName(), 1),
                Arguments.of(NewProduceRequestTransformation.class.getName(), 5),
                Arguments.of(NewCachingProduceRequestTransformation.class.getName(), 0),
                Arguments.of(NewCachingProduceRequestTransformation.class.getName(), 1),
                Arguments.of(NewCachingProduceRequestTransformation.class.getName(), 5));
    }

    @ParameterizedTest
    @MethodSource
    void measureLatency(String filterClass, int numFilters, KafkaCluster cluster, Topic topic) {
        runExperiment(filterClass, numFilters, cluster, topic, true);
        runExperiment(filterClass, numFilters, cluster, topic, false);
    }

    private static void runExperiment(String filterClass, int numFilters, KafkaCluster cluster, Topic topic, boolean warmup) {
        Request request = createProduceRequest(topic.topicId().orElseThrow());
        var config = proxy(cluster);
        for (int i = 0; i < numFilters; i++) {
            NamedFilterDefinition namedFilterDefinition = new NamedFilterDefinitionBuilder(filterClass + "-" + i,
                    filterClass)
                    .withConfig("transformation", UpperCasing.class.getName())
                    .withConfig("transformationConfig", Map.of("charset", "UTF8")).build();
            config.addToFilterDefinitions(namedFilterDefinition)
                    .addToDefaultFilters(namedFilterDefinition.name());
        }
        config.withNewManagement().withNewEndpoints().withNewPrometheus().endPrometheus().endEndpoints().endManagement();
        try (KroxyliciousTester kroxyliciousTester = KroxyliciousTesters.kroxyliciousTester(config);
                KafkaClient kafkaClient = kroxyliciousTester.simpleTestClient()) {
            repeatWithStatistics(filterClass, numFilters, kafkaClient, client -> {
                Response response = client.getSync(request);
                assertThat(response.payload().message()).isInstanceOfSatisfying(ProduceResponseData.class,
                        produceResponseData -> assertThat(produceResponseData.responses()).allSatisfy(
                                topicProduceResponse -> assertThat(topicProduceResponse.partitionResponses()).allSatisfy(
                                        topicProduceResponsePartitionResponse -> assertThat(Errors.forCode(topicProduceResponsePartitionResponse.errorCode())).isEqualTo(
                                                Errors.NONE))));
            }, kroxyliciousTester, warmup);
        }
    }

    private static void repeatWithStatistics(String filterClass, int numFilters, KafkaClient client, Consumer<KafkaClient> clientConsumer,
                                             KroxyliciousTester kroxyliciousTester, boolean warmup) {
        MeterRegistry registry = Metrics.globalRegistry;
        Timer.Builder builder = Timer.builder("metadata-timing").publishPercentiles(0.95, 0.99, 0.999);
        Timer timer = builder.register(registry);
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < MetadataTimingIT.REPETITIONS; i++) {
            stopwatch.reset();
            stopwatch.start();
            clientConsumer.accept(client);
            stopwatch.stop();
            timer.record(stopwatch.elapsed());
        }
        if (!warmup) {
            emitMetrics(filterClass, numFilters, kroxyliciousTester);
        }
    }

    private static void emitMetrics(String filterClass, int numFilters, KroxyliciousTester kroxyliciousTester) {
        List<SimpleMetric> simpleMetrics = kroxyliciousTester.getManagementClient().scrapeMetrics();
        double sentProduceRequests = simpleMetrics.stream().filter(metricName("kroxylicious_proxy_to_server_request_total"))
                .filter(hasLabel("api_key", "PRODUCE")).mapToDouble(SimpleMetric::value).sum();
        double sentMetadataRequests = simpleMetrics.stream().filter(metricName("kroxylicious_proxy_to_server_request_total"))
                .filter(hasLabel("api_key", "METADATA")).mapToDouble(SimpleMetric::value).sum();
        double receivedProduceResponses = simpleMetrics.stream().filter(metricName("kroxylicious_server_to_proxy_response_total"))
                .filter(hasLabel("api_key", "PRODUCE")).mapToDouble(SimpleMetric::value).sum();
        double receivedMetadataResponses = simpleMetrics.stream().filter(metricName("kroxylicious_server_to_proxy_response_total"))
                .filter(hasLabel("api_key", "METADATA")).mapToDouble(SimpleMetric::value).sum();
        double experimentSendCount = simpleMetrics.stream().filter(metricName("metadata_timing_seconds_count"))
                .mapToDouble(SimpleMetric::value).sum();
        double experimentSend999pSeconds = simpleMetrics.stream().filter(metricName("metadata_timing_seconds"))
                .filter(hasLabel("quantile", "0.999"))
                .mapToDouble(SimpleMetric::value).sum();
        double experimentSend99pSeconds = simpleMetrics.stream().filter(metricName("metadata_timing_seconds"))
                .filter(hasLabel("quantile", "0.99"))
                .mapToDouble(SimpleMetric::value).sum();
        double experimentSend95pSeconds = simpleMetrics.stream().filter(metricName("metadata_timing_seconds"))
                .filter(hasLabel("quantile", "0.95"))
                .mapToDouble(SimpleMetric::value).sum();
        double experimentSendMaxSeconds = simpleMetrics.stream().filter(metricName("metadata_timing_seconds_max"))
                .mapToDouble(SimpleMetric::value).sum();
        Stream<?> data = Stream.of(filterClass, numFilters, sentProduceRequests, sentMetadataRequests, receivedProduceResponses, receivedMetadataResponses,
                experimentSendCount, experimentSend999pSeconds, experimentSend99pSeconds, experimentSend95pSeconds, experimentSendMaxSeconds);
        System.out.println(data.map(Objects::toString).collect(Collectors.joining(",")));
    }

    private static Predicate<SimpleMetric> hasLabel(String key, String expectedValue) {
        return simpleMetric -> simpleMetric.labels().containsKey(key) && simpleMetric.labels().get(key).equals(expectedValue);
    }

    private static Predicate<SimpleMetric> metricName(String name) {
        return simpleMetric -> simpleMetric.name().equals(name);
    }

    private static Request createProduceRequest(Uuid topicId) {
        ProduceRequestData produceRequest = new ProduceRequestData();
        produceRequest.setAcks((short) 1);
        ProduceRequestData.TopicProduceData topicProduceData = new ProduceRequestData.TopicProduceData();
        topicProduceData.setName(null);
        topicProduceData.setTopicId(topicId);
        ProduceRequestData.PartitionProduceData data = new ProduceRequestData.PartitionProduceData();
        data.setIndex(0);
        data.setRecords(RecordTestUtils.singleElementMemoryRecords("k", "v"));
        topicProduceData.partitionData().add(data);
        ProduceRequestData.TopicProduceDataCollection coll = new ProduceRequestData.TopicProduceDataCollection();
        coll.mustAdd(topicProduceData);
        produceRequest.setTopicData(coll);
        return new Request(PRODUCE, (short) 13, "client", produceRequest);
    }

}