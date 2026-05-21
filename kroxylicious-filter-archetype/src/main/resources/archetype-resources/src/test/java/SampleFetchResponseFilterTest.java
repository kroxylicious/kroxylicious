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
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
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
 * Unit tests for {@link SampleFetchResponseFilter}.
 *
 * <p>These tests use {@link MockFilterContext} — Kroxylicious's tested emulation of
 * {@link io.kroxylicious.proxy.filter.FilterContext} — instead of building a Mockito mock by hand.
 * That avoids reimplementing the {@code FilterContext} contract in every test and keeps the
 * test code focused on filter behaviour.</p>
 */
class SampleFetchResponseFilterTest {

    private static final short API_VERSION = ApiMessageType.FETCH.highestSupportedVersion(true); // this is arbitrary for our filter
    private static final String TOPIC_NAME = "my-topic";
    private static final Uuid TOPIC_ID = new Uuid(7L, 7L);
    private static final String PRE_TRANSFORM_VALUE = "this is what the value will be transformed from";
    private static final String NO_TRANSFORM_VALUE = "this value will not be transformed";
    private static final String EXPECTED_TRANSFORMED_RECORD_VALUE = "[" + TOPIC_NAME + "] this is what the value will be transformed to";
    private static final String EXPECTED_NO_TRANSFORM_RECORD_VALUE = "[" + TOPIC_NAME + "] " + NO_TRANSFORM_VALUE;
    private static final String CONFIG_FIND_VALUE = "from";
    private static final String CONFIG_REPLACE_VALUE = "to";

    private SampleFetchResponseFilter filter;
    private ResponseHeaderData headerData;

    @BeforeEach
    public void beforeEach() {
        SampleFilterConfig config = new SampleFilterConfig(CONFIG_FIND_VALUE, CONFIG_REPLACE_VALUE);
        this.filter = new SampleFetchResponseFilter(config);
        this.headerData = new ResponseHeaderData();
    }

    /**
     * Unit Test: Checks that the transformation is applied (find/replace + topic-name prefix) when the
     * response carries the topic name directly.
     */
    @Test
    void willTransformFetchResponseUsingTopicNameTest() {
        var responseData = buildFetchResponseData(PRE_TRANSFORM_VALUE, topic -> topic.setTopic(TOPIC_NAME));
        MockFilterContext context = MockFilterContext.builder(headerData, responseData).build();

        var stage = filter.onFetchResponse(API_VERSION, headerData, responseData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(FetchResponseData.class, forwarded -> {
                        var unpackedResponse = unpackFetchResponseData(forwarded);
                        assertThat(unpackedResponse)
                                .doesNotContain(PRE_TRANSFORM_VALUE)
                                .containsExactly(EXPECTED_TRANSFORMED_RECORD_VALUE);
                    });
        });
    }

    /**
     * Unit Test: Checks that the topic-name prefix is applied even when no find/replace match occurs.
     */
    @Test
    void wontTransformFetchResponseTest() {
        var responseData = buildFetchResponseData(NO_TRANSFORM_VALUE, topic -> topic.setTopic(TOPIC_NAME));
        MockFilterContext context = MockFilterContext.builder(headerData, responseData).build();

        var stage = filter.onFetchResponse(API_VERSION, headerData, responseData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(FetchResponseData.class, forwarded -> {
                        var unpackedResponse = unpackFetchResponseData(forwarded);
                        assertThat(unpackedResponse).containsExactly(EXPECTED_NO_TRANSFORM_RECORD_VALUE);
                    });
        });
    }

    /**
     * Unit Test: Checks that when a transformation is applied, the response record(s) metadata
     * (offset, timestamp, key, headers) is preserved.
     */
    @Test
    void onTransformMetadataRetainedFetchResponseTest() {
        var responseData = buildFetchResponseData(PRE_TRANSFORM_VALUE, topic -> topic.setTopic(TOPIC_NAME));
        var sent = responseData.duplicate(); // due to pass-by-reference issues we need a safe copy to compare with
        MockFilterContext context = MockFilterContext.builder(headerData, responseData).build();

        var stage = filter.onFetchResponse(API_VERSION, headerData, responseData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(FetchResponseData.class, forwarded -> compareRecords(forwarded, sent));
        });
    }

    /**
     * Unit Test: Demonstrates the topic-id lookup pattern. The response carries only a {@code topicId}
     * (the latest Fetch RPC behaviour) and the filter is expected to resolve the topic name via
     * {@link io.kroxylicious.proxy.filter.FilterContext#topicNames} before applying its transformation.
     */
    @Test
    void willResolveTopicNameFromTopicIdTest() {
        var responseData = buildFetchResponseData(PRE_TRANSFORM_VALUE, topic -> topic.setTopicId(TOPIC_ID));
        MockFilterContext context = MockFilterContext.builder(headerData, responseData)
                .withTopicName(TOPIC_ID, TOPIC_NAME)
                .build();

        var stage = filter.onFetchResponse(API_VERSION, headerData, responseData, context);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(FetchResponseData.class, forwarded -> {
                        var unpackedResponse = unpackFetchResponseData(forwarded);
                        // Value is both transformed (from→to) AND prefixed with the topic name resolved from the id
                        assertThat(unpackedResponse)
                                .doesNotContain(PRE_TRANSFORM_VALUE)
                                .containsExactly(EXPECTED_TRANSFORMED_RECORD_VALUE);
                    });
        });
    }

    /**
     * Builds a FetchResponseData object from a given string. The string provided will then be unwrapped by
     * the SampleFetchResponseFilter during the test, after which it will attempt to perform a
     * transformation on that string based on the provided configuration.
     * @param transformValue  the content to be embedded in the response and transformed (or not) in the test
     * @param topicConfigurer applied to the synthesized topic response to set either {@code topic} or {@code topicId}
     * @return the FetchResponseData object to be passed to the SampleFetchResponseFilter during the test
     */
    private static FetchResponseData buildFetchResponseData(String transformValue,
                                                            java.util.function.Consumer<FetchResponseData.FetchableTopicResponse> topicConfigurer) {
        var responseData = new FetchResponseData();
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
        var partitions = new ArrayList<FetchResponseData.PartitionData>();
        var partitionData = new FetchResponseData.PartitionData();
        partitionData.setRecords(records);
        partitions.add(partitionData);
        // Build responses from built partitions
        var responses = new ArrayList<FetchResponseData.FetchableTopicResponse>();
        var response = new FetchResponseData.FetchableTopicResponse();
        response.setPartitions(partitions);
        topicConfigurer.accept(response);
        responses.add(response);
        // Add built responses to FetchResponseData object so that we can return it
        responseData.setResponses(responses);
        return responseData;
    }

    private static List<String> unpackFetchResponseData(FetchResponseData responseData) {
        var values = new ArrayList<String>();
        var records = unpackRecords(responseData);
        assertThat(records).isNotNull();
        for (Record record : records) {
            values.add(new String(StandardCharsets.UTF_8.decode(record.value()).array()));
        }
        return values;
    }

    private static List<Record> unpackRecords(FetchResponseData responseData) {
        List<Record> records = new ArrayList<>();
        for (FetchResponseData.FetchableTopicResponse response : responseData.responses()) {
            for (FetchResponseData.PartitionData partitionData : response.partitions()) {
                for (Record record : ((Records) partitionData.records()).records()) {
                    records.add(record);
                }
            }
        }
        return records;
    }

    private static void compareRecords(FetchResponseData a, FetchResponseData b) {
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
