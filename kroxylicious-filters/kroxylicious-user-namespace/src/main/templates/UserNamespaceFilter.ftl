<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
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
package ${outputPackage};

import java.util.Set;
import java.util.concurrent.CompletionStage;

<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
</#list>

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


import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
* Decodes Kafka Readable into an ApiMessage
* <p>Note: this class is automatically generated from a template</p>
*/
public class UserNamespaceFilter implements RequestFilter, ResponseFilter {

    private final UserNamespace.SampleFilterConfig config;

    private final Set<ApiKeys> keys = Set.of(FIND_COORDINATOR, OFFSET_COMMIT, CONSUMER_GROUP_DESCRIBE, DESCRIBE_GROUPS);

    UserNamespaceFilter(UserNamespace.SampleFilterConfig config) {
        this.config = config;
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        // Find coordinator uses identifies entities using a key type
        if (apiKey == FIND_COORDINATOR) {
            return true;
        }
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
        <#if messageSpec.entityFields.hasAtLeastOneEntityField  >
          case ${retrieveApiKey(messageSpec)} -> inVersion(apiVersion, Set.of(<#list messageSpec.entityFields.orderedVersions as version> (short) ${version}<#sep>, </#list>));
        </#if>
    </#if>
</#list>
            default -> false;
        };
    }

    private static boolean inVersion(short apiVersions, Set<Short> versions) {
        return versions.contains(apiVersions);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return keys.contains(apiKey);
    }

    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        var authzId = context.clientSaslContext().map(ClientSaslContext::authorizationId);
        authzId.ifPresent(aid -> {

            // Exceptions
            switch (apiKey) {
                case FIND_COORDINATOR -> {
                    FindCoordinatorRequestData findCoordinatorRequestData = (FindCoordinatorRequestData) request;
                    if (findCoordinatorRequestData.keyType() == 0 /* CHECK ME */) {
                        findCoordinatorRequestData.setCoordinatorKeys(findCoordinatorRequestData.coordinatorKeys().stream().map(k -> aid + "-" + k).toList());
                    }
                    System.out.println(findCoordinatorRequestData);
                }
<#--                case OFFSET_COMMIT -> {-->
<#--                    OffsetCommitRequestData offsetCommitRequestData = (OffsetCommitRequestData) request;-->
<#--                    offsetCommitRequestData.setGroupId(aid + "-" + offsetCommitRequestData.groupId());-->
<#--                    System.out.println(offsetCommitRequestData);-->
<#--                }-->
<#--                case CONSUMER_GROUP_DESCRIBE -> {-->
<#--                    ConsumerGroupDescribeRequestData consumerGroupDescribeRequestData = (ConsumerGroupDescribeRequestData) request;-->
<#--                    consumerGroupDescribeRequestData.setGroupIds(consumerGroupDescribeRequestData.groupIds().stream().map(g -> aid + "-" + g).toList());-->
<#--                    System.out.println(consumerGroupDescribeRequestData);-->
<#--                }-->
<#--                case DESCRIBE_GROUPS -> {-->
<#--                    DescribeGroupsRequestData describeGroupsRequestData = (DescribeGroupsRequestData) request;-->
<#--                    describeGroupsRequestData.setGroups(describeGroupsRequestData.groups().stream().map(g -> aid + "-" + g).toList());-->
<#--                    System.out.println(request);-->
<#--                }-->
            }

            <#macro dumpFoo messageSpec entityFields>
                <#-- entity fields of this node -->
              <#assign dataClass="${messageSpec.name}Data"
                dataVar="${messageSpec.name?uncap_first}Data"/>
                var ${dataVar} = (${dataClass}) request;
              <#list entityFields.entities as entity>

               <#assign getter="${entity.name?uncap_first}"
                        setter="set${entity.name}"
               />
               // ${messageSpec.name} ${entity.name} ${entity.type} ${entity.type.isArray?string('true', 'false')}
                if (inVersion(header.requestApiVersion(), Set.of(<#list messageSpec.validVersions.intersect(entity.versions) as version> (short) ${version}<#sep>, </#list>))) {
               <#if entity.type == 'string'>
                    ${dataVar}.${setter}(map(aid, ${dataVar}.${getter}()));
               <#elseif entity.type == '[]string'>
                    ${dataVar}.${setter}(${dataVar}.${getter}().stream().map(orig -> map(aid, orig)).toList());
               </#if>
                }
              </#list>
<#--
                <@dumpFoo childFoo />
-->
            </#macro>


            switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
        <#if messageSpec.entityFields.hasAtLeastOneEntityField  >
        case ${retrieveApiKey(messageSpec)} -> {
          <@dumpFoo messageSpec messageSpec.entityFields />
        }
        </#if>
    </#if>
</#list>
            };


        });
        return context.forwardRequest(header, request);

    }

    private static String map(String authId, String originalName) {
        return authId + "-" + originalName;
    }

    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
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


    /**
    * Decodes Kafka request Readable into an ApiMessage
    * @param apiKey the api key of the message
    * @param apiVersion the api version of the message
    * @param accessor the accessor for the message bytes
    * @return the ApiMessage
    * @throws IllegalArgumentException if an unhandled ApiKey is encountered
    */
    /*
    static ApiMessage decodeRequest(ApiKeys apiKey, short apiVersion, ByteBufAccessor accessor) {
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request'>
            case ${retrieveApiKey(messageSpec)} ->
                    new ${messageSpec.name}Data(accessor, apiVersion);
    </#if>
</#list>
            default -> throw new IllegalArgumentException("Unsupported RPC " + apiKey);
        };
    }
    */

    /**
    * Decodes Kafka response Readable into an ApiMessage
    * @param apiKey the api key of the message
    * @param apiVersion the api version of the message
    * @param accessor the accessor for the message bytes
    * @return the ApiMessage
    * @throws IllegalArgumentException if an unhandled ApiKey is encountered
    */
    /*
    static ApiMessage decodeResponse(ApiKeys apiKey, short apiVersion, ByteBufAccessor accessor) {
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response'>
        case ${retrieveApiKey(messageSpec)} -> new ${messageSpec.name}Data(accessor, apiVersion);
    </#if>
</#list>
            default -> throw new IllegalArgumentException("Unsupported RPC " + apiKey);
        };
    }
    */

}