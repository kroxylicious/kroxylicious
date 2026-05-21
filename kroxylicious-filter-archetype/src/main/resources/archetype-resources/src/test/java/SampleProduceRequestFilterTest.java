package ${package};

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.testing.filter.assertj.MockFilterContextAssert;
import io.kroxylicious.testing.filter.context.MockFilterContext;
import ${package}.config.SampleFilterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SampleProduceRequestFilter}.
 *
 * <p>These tests use {@link MockFilterContext} — Kroxylicious's tested emulation of
 * {@link io.kroxylicious.proxy.filter.FilterContext} — instead of building a Mockito mock by hand.
 * That avoids reimplementing the {@code FilterContext} contract in every test and keeps the
 * test code focused on filter behaviour.</p>
 */
class SampleProduceRequestFilterTest {

    private static final short API_VERSION = ApiMessageType.PRODUCE.highestSupportedVersion(true); // this is arbitrary for our filter
    private static final String TOPIC_NAME = "my-topic";
    private static final Uuid TOPIC_ID = new Uuid(5L, 5L);
    private static final String PRE_TRANSFORM_VALUE = "this is what the value will be transformed from";
    private static final String NO_TRANSFORM_VALUE = "this value will not be transformed";
    private static final String EXPECTED_TRANSFORMED_RECORD_VALUE = "[" + TOPIC_NAME + "] this is what the value will be transformed to";
    private static final String EXPECTED_NO_TRANSFORM_RECORD_VALUE = "[" + TOPIC_NAME + "] " + NO_TRANSFORM_VALUE;
    private static final String CONFIG_FIND_VALUE = "from";
    private static final String CONFIG_REPLACE_VALUE = "to";

    private SampleProduceRequestFilter filter;
    private RequestHeaderData headerData;

    @BeforeEach
    public void beforeEach() {
        SampleFilterConfig config = new SampleFilterConfig(CONFIG_FIND_VALUE, CONFIG_REPLACE_VALUE);
        this.filter = new SampleProduceRequestFilter(config);
        this.headerData = new RequestHeaderData();
    }

    /**
     * Unit Test: Checks that the transformation is applied (find/replace + topic-name prefix) when the
     * request carries the topic name directly.
     */
    @Test
    void willTransformProduceRequestUsingTopicNameTest() {
        var requestData = buildProduceRequestData(PRE_TRANSFORM_VALUE, topic -> topic.setName(TOPIC_NAME));
        MockFilterContext context = MockFilterContext.builder(headerData, requestData).build();

        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardRequest()
                    .hasMessageInstanceOfSatisfying(ProduceRequestData.class, forwarded -> {
                        var unpackedRequest = unpackProduceRequestData(forwarded);
                        assertThat(unpackedRequest)
                                .doesNotContain(PRE_TRANSFORM_VALUE)
                                .containsExactly(EXPECTED_TRANSFORMED_RECORD_VALUE);
                    });
        });
    }

    /**
     * Unit Test: Checks that the topic-name prefix is applied even when no find/replace match occurs.
     */
    @Test
    void wontTransformProduceRequestTest() {
        var requestData = buildProduceRequestData(NO_TRANSFORM_VALUE, topic -> topic.setName(TOPIC_NAME));
        MockFilterContext context = MockFilterContext.builder(headerData, requestData).build();

        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardRequest()
                    .hasMessageInstanceOfSatisfying(ProduceRequestData.class, forwarded -> {
                        var unpackedRequest = unpackProduceRequestData(forwarded);
                        assertThat(unpackedRequest).containsExactly(EXPECTED_NO_TRANSFORM_RECORD_VALUE);
                    });
        });
    }

    /**
     * Unit Test: Checks that when a transformation is applied, the request record(s) metadata
     * (offset, timestamp, key, headers) is preserved.
     */
    @Test
    void onTransformMetadataRetainedProduceRequestTest() {
        var requestData = buildProduceRequestData(PRE_TRANSFORM_VALUE, topic -> topic.setName(TOPIC_NAME));
        var sent = requestData.duplicate(); // due to pass-by-reference issues we need a safe copy to compare with
        MockFilterContext context = MockFilterContext.builder(headerData, requestData).build();

        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardRequest()
                    .hasMessageInstanceOfSatisfying(ProduceRequestData.class, forwarded -> compareRecords(forwarded, sent));
        });
    }

    /**
     * Unit Test: Demonstrates the topic-id lookup pattern. The request carries only a {@code topicId}
     * (the latest Produce RPC behaviour) and the filter is expected to resolve the topic name via
     * {@link io.kroxylicious.proxy.filter.FilterContext#topicNames} before applying its transformation.
     */
    @Test
    void willResolveTopicNameFromTopicIdTest() {
        var requestData = buildProduceRequestData(PRE_TRANSFORM_VALUE, topic -> topic.setTopicId(TOPIC_ID));
        MockFilterContext context = MockFilterContext.builder(headerData, requestData)
                .withTopicName(TOPIC_ID, TOPIC_NAME)
                .build();

        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardRequest()
                    .hasMessageInstanceOfSatisfying(ProduceRequestData.class, forwarded -> {
                        var unpackedRequest = unpackProduceRequestData(forwarded);
                        // Value is both transformed (foo→bar) AND prefixed with the topic name resolved from the id
                        assertThat(unpackedRequest)
                                .doesNotContain(PRE_TRANSFORM_VALUE)
                                .containsExactly(EXPECTED_TRANSFORMED_RECORD_VALUE);
                    });
        });
    }

    private static ProduceRequestData buildProduceRequestData(String transformValue,
                                                              java.util.function.Consumer<ProduceRequestData.TopicProduceData> topicConfigurer) {
        var requestData = new ProduceRequestData();
        // Build stream
        var stream = new ByteBufferOutputStream(ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)));
        // Build records from stream
        var recordsBuilder = new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, 0,
                RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, stream.remaining());
        // Create record Headers
        Header header = new RecordHeader("myKey", "myValue".getBytes());
        // Add transformValue as buffer to records
        recordsBuilder.append(RecordBatch.NO_TIMESTAMP, null, ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)).position(0), new Header[]{ header });
        var records = recordsBuilder.build();
        // Build partitions from built records
        var partitions = new ArrayList<ProduceRequestData.PartitionProduceData>();
        var partitionData = new ProduceRequestData.PartitionProduceData();
        partitionData.setRecords(records);
        partitions.add(partitionData);
        // Build topics from built partitions
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        var topicData = new ProduceRequestData.TopicProduceData();
        topicData.setPartitionData(partitions);
        topicConfigurer.accept(topicData);
        topics.add(topicData);
        // Add built topics to ProduceRequestData object so that we can return it
        requestData.setTopicData(topics);
        return requestData;
    }

    private static List<String> unpackProduceRequestData(ProduceRequestData requestData) {
        var recordList = unpackRecords(requestData);
        var values = new ArrayList<String>();
        for (Record record : recordList) {
            values.add(new String(StandardCharsets.UTF_8.decode(record.value()).array()));
        }
        return values;
    }

    private static List<Record> unpackRecords(ProduceRequestData requestData) {
        List<Record> records = new ArrayList<>();
        for (ProduceRequestData.TopicProduceData topicProduceData : requestData.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {
                for (Record record : ((Records) partitionProduceData.records()).records()) {
                    records.add(record);
                }
            }
        }
        return records;
    }

    private static void compareRecords(ProduceRequestData a, ProduceRequestData b) {
        var aRecords = unpackRecords(a).listIterator();
        var bRecords = unpackRecords(b).listIterator();
        assertThat(aRecords).isNotNull();
        assertThat(bRecords).isNotNull();
        while (aRecords.hasNext()) {
            assertThat(bRecords).hasNext();
            checkRecordMetadataMatches(aRecords.next(), bRecords.next());
        }
    }

    private static void checkRecordMetadataMatches(Record a, Record b) {
        // We don't compare record size, value, or value size as we expect the filter to change these
        assertThat(a).usingRecursiveComparison()
                .ignoringFields("sizeInBytes", "valueSize", "value")
                .isEqualTo(b);
        assertThat(a.headers()).usingRecursiveComparison().isEqualTo(b.headers());
    }
}
