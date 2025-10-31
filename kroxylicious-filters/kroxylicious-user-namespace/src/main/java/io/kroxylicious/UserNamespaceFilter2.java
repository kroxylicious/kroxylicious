package io.kroxylicious;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static org.apache.kafka.common.protocol.ApiKeys.CONSUMER_GROUP_DESCRIBE;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_GROUPS;
import static org.apache.kafka.common.protocol.ApiKeys.FIND_COORDINATOR;
import static org.apache.kafka.common.protocol.ApiKeys.OFFSET_COMMIT;

/**
 * A sample ProduceRequestFilter implementation, intended to demonstrate how custom filters work with
 * Kroxylicious.<br />
 * <br />
 * This filter transforms the partition data sent by a Kafka producer in a produce request by replacing all
 * occurrences of the String "foo" with the String "bar". These strings are configurable in the config file,
 * so you could substitute this with any text you want.<br />
 * <br />
 * An example of a use case where this might be applicable is when producers are sending data to Kafka
 * using different formats from what consumers are expecting. You could configure this filter to transform
 * the data sent by producers to Kafka into the format consumers expect. In this example use case, the filter
 * could be further modified to apply different transformations to different topics, or when sent by
 * particular producers.
 */
class UserNamespaceFilter2 implements RequestFilter, ResponseFilter {

    private final UserNamespace.SampleFilterConfig config;

    private final Set<ApiKeys> keys = Set.of(FIND_COORDINATOR, OFFSET_COMMIT, CONSUMER_GROUP_DESCRIBE, DESCRIBE_GROUPS);

    UserNamespaceFilter2(UserNamespace.SampleFilterConfig config) {
        this.config = config;
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return keys.contains(apiKey);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return keys.contains(apiKey);
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        var authzId = context.clientSaslContext().map(ClientSaslContext::authorizationId);
        authzId.ifPresent(aid -> {

            switch (apiKey) {
                case FIND_COORDINATOR -> {
                    FindCoordinatorRequestData findCoordinatorRequestData = (FindCoordinatorRequestData) request;
                    if (findCoordinatorRequestData.keyType() == 0 /* CHECK ME */) {
                        findCoordinatorRequestData.setCoordinatorKeys(findCoordinatorRequestData.coordinatorKeys().stream().map(k -> aid + "-" + k).toList());
                    }
                    System.out.println(findCoordinatorRequestData);
                }
                case OFFSET_COMMIT -> {
                    OffsetCommitRequestData offsetCommitRequestData = (OffsetCommitRequestData) request;
                    offsetCommitRequestData.setGroupId(aid + "-" + offsetCommitRequestData.groupId());
                    System.out.println(offsetCommitRequestData);
                }
                case CONSUMER_GROUP_DESCRIBE -> {
                    ConsumerGroupDescribeRequestData consumerGroupDescribeRequestData = (ConsumerGroupDescribeRequestData) request;
                    consumerGroupDescribeRequestData.setGroupIds(consumerGroupDescribeRequestData.groupIds().stream().map(g -> aid + "-" + g).toList());
                    System.out.println(consumerGroupDescribeRequestData);
                }
                case DESCRIBE_GROUPS -> {
                    DescribeGroupsRequestData describeGroupsRequestData = (DescribeGroupsRequestData) request;
                    describeGroupsRequestData.setGroups(describeGroupsRequestData.groups().stream().map(g -> aid + "-" + g).toList());
                    System.out.println(request);
                }
            }
        });
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
        var authzId = context.clientSaslContext().map(ClientSaslContext::authorizationId);
        authzId.ifPresent(aid -> {
            switch (apiKey) {
                case FIND_COORDINATOR -> {
                    FindCoordinatorResponseData findCoordinatorResponseData = (FindCoordinatorResponseData) response;
                    findCoordinatorResponseData.coordinators().forEach(
                            coordinator -> coordinator.setKey(coordinator.key().substring(aid.length() + 1)));
                    System.out.println(findCoordinatorResponseData);
                }
                case OFFSET_COMMIT -> {
                    OffsetCommitResponseData offsetCommitResponseData = (OffsetCommitResponseData) response;
                    System.out.println(response);
                }
                case CONSUMER_GROUP_DESCRIBE -> {
                    ConsumerGroupDescribeResponseData consumerGroupDescribeResponseData = (ConsumerGroupDescribeResponseData) response;
                    consumerGroupDescribeResponseData.groups().forEach(group -> {
                        group.setGroupId(group.groupId().substring(aid.length() + 1));
                    });
                    System.out.println(response);
                }
                case DESCRIBE_GROUPS -> {
                    DescribeGroupsResponseData describeGroupsResponseData = (DescribeGroupsResponseData) response;
                    describeGroupsResponseData.groups().forEach(g -> {
                        g.setGroupId(g.groupId().substring(aid.length() + 1));
                    });
                    System.out.println(response);
                }

            }
        });

        return context.forwardResponse(header, response);
    }
}
