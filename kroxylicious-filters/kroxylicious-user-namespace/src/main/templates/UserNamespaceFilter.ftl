<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#macro mapRequestFields messageSpec dataVar fields indent>
    <#local pad = ""?left_pad(4*indent)/>
${pad}// process any entity fields defined at this level
    <#list fields as field>
        <#if field.entityType == 'GROUP_ID' || field.entityType == 'TRANSACTIONAL_ID' || field.entityType == 'TOPIC_NAME'>
            <#local getter="${field.name?uncap_first}" setter="set${field.name}" />
${pad}if (shouldMap("${field.entityType}") && inVersion(header.requestApiVersion(), Set.of(<#list messageSpec.validVersions.intersect(field.versions) as version> (short) ${version}<#sep>, </#list>))) {
            <#if field.type == 'string'>
${pad}    ${dataVar}.${setter}(map(aid, ${dataVar}.${getter}()));
            <#elseif field.type == '[]string'>
${pad}    ${dataVar}.${setter}(${dataVar}.${getter}().stream().map(orig -> map(aid, orig)).toList());
            </#if>
${pad}}
        </#if>
    </#list>
${pad}// recursively process any sub fields
    <#list fields as field>
        <#if field.type.isArray && field.fields?size != 0 >
            <#local collection="${field.name?uncap_first}" />
            <#local subvar=collection[0..<collection?length-1] />
${pad}${dataVar}.${collection}().forEach(${subvar} -> {
            <@mapRequestFields messageSpec subvar field.fields indent + 1 />
${pad}});
        </#if>
    </#list>
</#macro>
<#macro mapAndFilterResponseFields messageSpec dataVar fields indent>
    <#local pad = ""?left_pad(4*indent)/>
${pad}// process any entity fields defined at this level
    <#list fields as field>
        <#if field.entityType == 'GROUP_ID' || field.entityType == 'TRANSACTIONAL_ID' || field.entityType == 'TOPIC_NAME'>
            <#local getter="${field.name?uncap_first}" setter="set${field.name}" />
${pad}if (shouldMap("${field.entityType}") && inVersion(apiVersion, Set.of(<#list messageSpec.validVersions.intersect(field.versions) as version> (short) ${version}<#sep>, </#list>))) {
            <#if field.type == 'string'>
${pad}    ${dataVar}.${setter}(unmap(aid, ${dataVar}.${getter}()));
            <#elseif field.type == '[]string'>
${pad}    ${dataVar}.${setter}(${dataVar}.${getter}().stream().map(orig -> unmap(aid, orig)).toList());
            </#if>
${pad}}
        </#if>
    </#list>
${pad}// recursively process any sub fields
    <#list fields as field>
        <#if field.type.isArray && field.fields?size != 0 >
            <#local collection="${field.name?uncap_first}" />
            <#local subvar=collection[0..<collection?length-1] />
${pad}${dataVar}.${collection}().forEach(${subvar} -> {
            <@mapAndFilterResponseFields messageSpec subvar field.fields indent + 1 />
${pad}});
        </#if>
    </#list>
</#macro>
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request' && (messageSpec.hasAtLeastOneEntityField || messageSpec.name == 'FindCoordinatorRequest')>
import org.apache.kafka.common.message.${messageSpec.name}Data;
    </#if>
    <#if messageSpec.type?lower_case == 'response' && (messageSpec.hasAtLeastOneEntityField || messageSpec.name == 'FindCoordinatorResponse')>
import org.apache.kafka.common.message.${messageSpec.name}Data;
    </#if>
</#list>

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import static org.apache.kafka.common.protocol.ApiKeys.FIND_COORDINATOR;


/**
* User nampespace filter.
* <p>Note: this class is automatically generated from a template</p>
*/
public class UserNamespaceFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserNamespaceFilter.class);

    <#list messageSpecs as messageSpec>
        <#if messageSpec.type?lower_case == 'request' && messageSpec.hasAtLeastOneEntityField>
            <#assign specName>
            <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
            </#assign>
    private static final Set<Short> ${specName?trim}_VERSIONS = Set.of(<#list messageSpec.entityFieldIntersectedVersions as version>(short) ${version}<#sep>, </#list>);
        </#if>
    </#list>

    <#list messageSpecs as messageSpec>
        <#if messageSpec.type?lower_case == 'response' && messageSpec.hasAtLeastOneEntityField>
            <#assign specName>
            <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
            </#assign>
    private static final Set<Short> ${specName?trim}_VERSIONS = Set.of(<#list messageSpec.entityFieldIntersectedVersions as version>(short) ${version}<#sep>, </#list>);
        </#if>
    </#list>

    private final UserNamespace.Config config;
    private final Map<ApiKeys, Short> responseApiKeyMap = new HashMap<>();

    UserNamespaceFilter(UserNamespace.Config config) {
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
    <#if messageSpec.type?c_lower_case == 'request' && messageSpec.hasAtLeastOneEntityField>
        <#assign specName>
            <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
        </#assign>
            case ${retrieveApiKey(messageSpec)} -> inVersion(apiVersion, ${specName?trim}_VERSIONS);
    </#if>
</#list>
            default -> false;
        };
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        boolean should = doShouldHandleResponse(apiKey, apiVersion);
        if (should) {
            responseApiKeyMap.put(apiKey, apiVersion);
        }
        return should;
    }

    private static boolean doShouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        // Find coordinator uses identifies entities using a key type
        if (apiKey == FIND_COORDINATOR) {
            return true;
        }
        return switch (apiKey) {
<#list messageSpecs as messageSpec>
                <#if messageSpec.type?c_lower_case == 'response' && messageSpec.hasAtLeastOneEntityField>
                    <#assign specName>
                        <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${
                    word?c_upper_case}<#sep>_</#list>
                    </#assign>
            case ${retrieveApiKey(messageSpec)} -> inVersion(apiVersion, ${specName?trim}_VERSIONS);
            </#if>
        </#list>
        default -> false;
        };
    }

    private static boolean inVersion(short apiVersions, Set<Short> versions) {
        return versions.contains(apiVersions);
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
                    LOGGER.atDebug().addArgument(findCoordinatorRequestData).log("find coordinator request: {}");
                    if (config.resourceTypes().contains(UserNamespace.ResourceType.GROUP_ID) &&  findCoordinatorRequestData.keyType() == 0 /* CHECK ME */) {
                        findCoordinatorRequestData.setCoordinatorKeys(findCoordinatorRequestData.coordinatorKeys().stream().map(k -> map(aid, k)).toList());
                    }
                    LOGGER.atDebug().addArgument(findCoordinatorRequestData).log("find coordinator request result: {}");
                }
            }


            switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'request' && messageSpec.hasAtLeastOneEntityField>
                case ${retrieveApiKey(messageSpec)} -> {
                    <#assign specName>
                      <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
                    </#assign>
                    <#assign dataClass="${messageSpec.name}Data" dataVar="${messageSpec.name?uncap_first}Data"/>
                    var ${dataVar} = (${dataClass}) request;
                    LOGGER.atDebug()
                            .addArgument(context.sessionId())
                            .addArgument(aid)
                            .addArgument(${dataVar})
                            .log("{} for {}: request ${specName?trim}: {}");
                    <@mapRequestFields messageSpec dataVar messageSpec.fields 5/>
                    LOGGER.atDebug()
                            .addArgument(context.sessionId())
                            .addArgument(aid)
                            .addArgument(${dataVar})
                            .log("{} for {}: request result ${specName?trim}: {}");
                }
    </#if>
</#list>
            }
        });
        return context.forwardRequest(header, request);
    }

    private boolean shouldMap(String entityType) {
        try {
            return config.resourceTypes().contains(UserNamespace.ResourceType.valueOf(entityType));
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static String map(String authId, String originalName) {
        if (originalName.isEmpty()) {
            return originalName;
        }
        return authId + "-" + originalName;
    }

    private static String unmap(String authId, String mappedName) {
        if (mappedName.isEmpty()) {
            return mappedName;
        }
        if (!mappedName.startsWith(authId + "-")) {
           throw new IllegalStateException("Mapped name does not have expected prefix: " + mappedName);
        }
        return mappedName.substring(authId.length() + 1);
    }


    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        var apiVersion = Objects.requireNonNull(responseApiKeyMap.get(apiKey)); // Workaround for https://github.com/kroxylicious/kroxylicious/issues/2916
        var authzId = context.clientSaslContext().map(ClientSaslContext::authorizationId);
        authzId.ifPresent(aid -> {
            switch (apiKey) {
                case FIND_COORDINATOR -> {
                    FindCoordinatorResponseData findCoordinatorResponseData = (FindCoordinatorResponseData) response;
                    findCoordinatorResponseData.coordinators().forEach(
                            coordinator -> coordinator.setKey(unmap(aid, coordinator.key())));
                    LOGGER.atDebug()
                            .addArgument(context.sessionId())
                            .addArgument(authzId.get())
                            .addArgument(findCoordinatorResponseData)
                            .log("{} for {}: find coordinator response result: {}");
                }
            }

            switch (apiKey) {
<#list messageSpecs as messageSpec>
    <#if messageSpec.type?lower_case == 'response' && messageSpec.hasAtLeastOneEntityField>
                case ${retrieveApiKey(messageSpec)} -> {
                    <#assign specName>
                        <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
                    </#assign>
                    <#assign dataClass="${messageSpec.name}Data" dataVar="${messageSpec.name?uncap_first}Data"/>
                        var ${dataVar} = (${dataClass}) response;
                    LOGGER.atDebug()
                            .addArgument(context.sessionId())
                            .addArgument(aid)
                            .addArgument(${dataVar})
                            .log("{} for {}: response ${specName?trim}: {}");
                    <@mapAndFilterResponseFields messageSpec dataVar messageSpec.fields 5/>
                    LOGGER.atDebug()
                            .addArgument(context.sessionId())
                            .addArgument(aid)
                            .addArgument(${dataVar})
                            .log("{} for {}: response result ${specName?trim}: {}");
                }
    </#if>
</#list>
            }
        });
        return context.forwardRequest(header, request);


<#--            switch (apiKey) {-->
<#--                case OFFSET_COMMIT -> {-->
<#--                    OffsetCommitResponseData offsetCommitResponseData = (OffsetCommitResponseData) response;-->
<#--                    LOGGER.atDebug()-->
<#--                            .addArgument(context.sessionId())-->
<#--                            .addArgument(authzId.get())-->
<#--                            .addArgument(response)-->
<#--                            .log("{} for {}: offset commit response result: {}");-->
<#--                }-->
<#--                case CONSUMER_GROUP_DESCRIBE -> {-->
<#--                    ConsumerGroupDescribeResponseData consumerGroupDescribeResponseData = (ConsumerGroupDescribeResponseData) response;-->
<#--                    consumerGroupDescribeResponseData.groups().forEach(group -> {-->
<#--                        group.setGroupId(unmap(aid, group.groupId()));-->
<#--                    });-->
<#--                    LOGGER.atDebug()-->
<#--                            .addArgument(context.sessionId())-->
<#--                            .addArgument(authzId.get())-->
<#--                            .addArgument(response).log("{} for {}: consumer group describe response: {}");-->
<#--                }-->
<#--                case DESCRIBE_GROUPS -> {-->
<#--                    DescribeGroupsResponseData describeGroupsResponseData = (DescribeGroupsResponseData) response;-->
<#--                    describeGroupsResponseData.groups().forEach(g -> g.setGroupId(unmap(aid, g.groupId())));-->
<#--                    LOGGER.atDebug()-->
<#--                            .addArgument(context.sessionId())-->
<#--                            .addArgument(authzId.get())-->
<#--                            .addArgument(response).log("{} for {}: describe group response: {}");-->
<#--                }-->
<#--                case OFFSET_FETCH -> {-->
<#--                    OffsetFetchResponseData offsetFetchResponseData = (OffsetFetchResponseData) response;-->
<#--                    offsetFetchResponseData.groups().forEach(g -> g.setGroupId(unmap(aid, g.groupId())));-->
<#--                    LOGGER.atDebug()-->
<#--                            .addArgument(context.sessionId())-->
<#--                            .addArgument(authzId.get())-->
<#--                            .addArgument(response).log("{} for {}: offset fetch response: {}");-->
<#--                }-->
<#--            }-->
<#--        });-->

<#--        return context.forwardResponse(header, response);-->
    }
}