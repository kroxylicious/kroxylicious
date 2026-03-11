/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.DescribedGroup;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.Member;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

class ConsumerGroupDescribeEnforcement extends ApiEnforcement<ConsumerGroupDescribeRequestData, ConsumerGroupDescribeResponseData> {

    public static final List<Function<Member, Assignment>> ALL_ASSIGNMENTS = List.of(Member::assignment, Member::targetAssignment);
    private static final String AUTHZ_FAILED_MSG = "The group has described topic(s) that the client is not authorized to describe.";

    @Override
    short minSupportedVersion() {
        return 0;
    }

    @Override
    short maxSupportedVersion() {
        return 1;
    }

    record DeniedGroups(List<String> deniedGroups) implements InflightState<ConsumerGroupDescribeResponseData> {

        @Override
        public ConsumerGroupDescribeResponseData merge(ConsumerGroupDescribeResponseData consumerGroupDescribeResponseData) {
            return null;
        }
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   ConsumerGroupDescribeRequestData request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        List<Action> describeGroupActions = request.groupIds().stream().map(groupId -> new Action(GroupResource.DESCRIBE, groupId)).toList();
        return authorizationFilter.authorization(context, describeGroupActions).thenCompose(authorizeResult -> {
            if (authorizeResult.allowed().isEmpty()) {
                return context.requestFilterResultBuilder().errorResponse(header, request, Errors.GROUP_AUTHORIZATION_FAILED.exception()).completed();
            }
            else if (!authorizeResult.denied().isEmpty()) {
                Map<Decision, List<String>> groupsByDecision = authorizeResult.partition(request.groupIds(), GroupResource.DESCRIBE, Function.identity());
                List<String> deniedGroups = groupsByDecision.get(Decision.DENY);
                request.groupIds().removeAll(deniedGroups);
                authorizationFilter.pushInflightState(header, new DeniedGroups(deniedGroups));
                return context.forwardRequest(header, request);
            }
            else {
                return context.forwardRequest(header, request);
            }
        });
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                     ConsumerGroupDescribeResponseData response,
                                                     FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        DeniedGroups deniedGroups = authorizationFilter.popInflightState(header, DeniedGroups.class);
        if (deniedGroups != null) {
            for (String deniedGroup : deniedGroups.deniedGroups()) {
                DescribedGroup responseGroup = new DescribedGroup();
                responseGroup.setGroupId(deniedGroup);
                responseGroup.setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code());
                response.groups().add(responseGroup);
            }
        }
        List<Action> actions = response.groups().stream()
                .flatMap(maybeNullOrEmpty(DescribedGroup::members))
                .flatMap(maybeNull(ALL_ASSIGNMENTS))
                .flatMap(maybeNullOrEmpty(Assignment::topicPartitions))
                .distinct()
                .map(topicPart -> new Action(TopicResource.DESCRIBE, topicPart.topicName()))
                .toList();
        if (actions.isEmpty()) {
            return super.onResponse(header, response, context, authorizationFilter);
        }
        else {
            return authorizationFilter.authorization(context, actions).thenCompose(result -> {
                List<DescribedGroup> groupsContainingAnyDeniedTopic = response.groups().stream().filter(describedGroup -> anyTopicPartitionDenied(result, describedGroup))
                        .toList();
                response.groups().removeAll(groupsContainingAnyDeniedTopic);
                groupsContainingAnyDeniedTopic.forEach(denied -> {
                    DescribedGroup e = new DescribedGroup()
                            .setGroupId(denied.groupId())
                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                            .setErrorMessage(AUTHZ_FAILED_MSG)
                            .setMembers(List.of());
                    response.groups().add(e);
                });
                return super.onResponse(header, response, context, authorizationFilter);
            });
        }
    }

    private static boolean anyTopicPartitionDenied(AuthorizeResult result, DescribedGroup describedGroup) {
        return describedGroup.members() != null && describedGroup.members().stream().flatMap(maybeNull(ALL_ASSIGNMENTS)).flatMap(maybeNullOrEmpty(
                Assignment::topicPartitions))
                .anyMatch(topicPart -> result.decision(TopicResource.DESCRIBE, topicPart.topicName()) == Decision.DENY);
    }

    static <T, E> Function<T, Stream<E>> maybeNullOrEmpty(Function<T, Collection<E>> function) {
        return t -> {
            Collection<E> apply = function.apply(t);
            if (apply == null || apply.isEmpty()) {
                return Stream.empty();
            }
            return apply.stream();
        };
    }

    static <T, E> Function<T, Stream<E>> maybeNull(List<Function<T, E>> functions) {
        return t -> functions.stream().flatMap(teFunction -> Optional.ofNullable(teFunction.apply(t)).stream());
    }
}
