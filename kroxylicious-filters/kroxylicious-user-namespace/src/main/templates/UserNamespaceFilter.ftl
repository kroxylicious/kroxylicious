<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#macro fieldVersionSet messageSpec versions>
Set.of(<#list messageSpec.validVersions.intersect(versions) as version> (short) ${version}<#sep>, </#list>)</#macro>
<#macro mapRequestFields messageSpec dataVar fields indent>
    <#local pad = ""?left_pad(4*indent)/>
    <#list fields?filter(field -> field.entityType == 'GROUP_ID' || field.entityType == 'TRANSACTIONAL_ID' || field.entityType == 'TOPIC_NAME')>
${pad}// process any entity fields defined at this level
        <#items as field>
            <#local getter="${field.name?uncap_first}" setter="set${field.name}" />
${pad}if (shouldMap("${field.entityType}") && inVersion(header.requestApiVersion(), <@fieldVersionSet messageSpec field.versions/>)) {
            <#if field.type == 'string'>
${pad}    ${dataVar}.${setter}(map(aid, ${dataVar}.${getter}()));
            <#elseif field.type == '[]string'>
${pad}    ${dataVar}.${setter}(${dataVar}.${getter}().stream().map(orig -> map(aid, orig)).toList());
            </#if>
${pad}}
        </#items>
    </#list>
${pad}
    <#list fields?filter(field -> field.type.isArray && field.fields?size != 0) >
${pad}// recursively process sub-fields
        <#items as field>
            <#local getter="${field.name?uncap_first}()" />
            <#local elementVar=field.type?remove_beginning("[]")?uncap_first />
${pad}if (inVersion(header.requestApiVersion(), <@fieldVersionSet messageSpec field.versions/>) && ${dataVar}.${getter} != null) {
${pad}    ${dataVar}.${getter}.forEach(${elementVar} -> {
                <@mapRequestFields messageSpec elementVar field.fields indent + 1 />
${pad}    });
${pad}}
        </#items>
    </#list>
</#macro>
<#macro mapAndFilterResponseFields messageSpec collectionIterator dataVar dataClass fields indent>
    <#local pad = ""?left_pad(4*indent)/>
    <#list fields?filter(field -> field.entityType == 'GROUP_ID' || field.entityType == 'TRANSACTIONAL_ID' || field.entityType == 'TOPIC_NAME')>
${pad}// process entity fields defined at this level
        <#items as field>
            <#local getter="${field.name?uncap_first}" setter="set${field.name}" />
${pad}if (shouldMap("${field.entityType}") && inVersion(apiVersion, <@fieldVersionSet messageSpec field.versions/>)) {
            <#if field.type == 'string'>
${pad}    if (inNamespace(aid, ${dataVar}.${getter}())) {
${pad}        ${dataVar}.${setter}(unmap(aid, ${dataVar}.${getter}()));
${pad}    }
                <#if collectionIterator?has_content>
${pad}    else {
${pad}        ${collectionIterator}.remove();
${pad}    }
                </#if>
            <#elseif field.type == '[]string'>
${pad}    ${dataVar}.${setter}(${dataVar}.${getter}().stream()
${pad}                        .filter(orig -> inNamespace(aid, orig))
${pad}                        .map(orig -> unmap(aid, orig)).toList());
                <#if collectionIterator?has_content>
${pad}    if (!${dataVar}.${getter}().isEmpty()) {
${pad}        ${collectionIterator}.remove();
${pad}    }
                </#if>
            </#if>
${pad}}
        </#items>
    </#list>
    <#list fields?filter(field -> field.type.isArray && field.fields?size != 0) >
${pad}// recursively process sub-fields
        <#items as field>
            <#local getter="${field.name?uncap_first}" />
            <#local collectionIteratorVar=field.name?uncap_first + "Iterator" />
            <#local elementVar=field.type?remove_beginning("[]")?uncap_first />
${pad}if (inVersion(apiVersion, <@fieldVersionSet messageSpec field.versions/>) && ${dataVar}.${getter}() != null) {
${pad}    var ${collectionIteratorVar} = ${dataVar}.${getter}().iterator();
${pad}    while (${collectionIteratorVar}.hasNext()) {
${pad}       var ${elementVar} = ${collectionIteratorVar}.next();
            <@mapAndFilterResponseFields messageSpec collectionIteratorVar elementVar dataClass field.fields indent + 2 />
${pad}    }
${pad}}
        </#items>
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

import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.annotation.processing.Generated;

<#list messageSpecs?filter(ms -> retrieveApiListener(ms)?seq_contains("BROKER"))>
    <#items as messageSpec>
       <#if messageSpec.type?lower_case == 'request' && (messageSpec.hasAtLeastOneEntityField || messageSpec.name == 'FindCoordinatorRequest')>
import org.apache.kafka.common.message.${messageSpec.name}Data;
        </#if>
        <#if messageSpec.type?lower_case == 'response' && (messageSpec.hasAtLeastOneEntityField || messageSpec.name == 'FindCoordinatorResponse')>
import org.apache.kafka.common.message.${messageSpec.name}Data;
        </#if>
    </#items>
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
@Generated("io.kroxylicious.krpccodegen.main.KrpcGenerator")
public class UserNamespaceFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserNamespaceFilter.class);

    <#list messageSpecs?filter(ms -> ms.type?lower_case == 'request' && ms.hasAtLeastOneEntityField && ms.listeners?seq_contains("BROKER"))>
        <#items as messageSpec>
            <#assign specName>
                <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
            </#assign>
    private static final Set<Short> ${specName?trim}_VERSIONS = Set.of(<#list messageSpec.entityFieldIntersectedVersions as version>(short) ${version}<#sep>, </#list>);
        </#items>
    </#list>

    <#list messageSpecs?filter(ms -> ms.type?lower_case == 'response' && ms.hasAtLeastOneEntityField && retrieveApiListener(ms)?seq_contains("BROKER"))>
        <#items as messageSpec>
            <#assign specName>
            <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
            </#assign>
    private static final Set<Short> ${specName?trim}_VERSIONS = Set.of(<#list messageSpec.entityFieldIntersectedVersions as version>(short) ${version}<#sep>, </#list>);
        </#items>
    </#list>

    private final UserNamespace.Config config;

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
        <#list messageSpecs?filter(ms -> ms.type?lower_case == 'request' && ms.hasAtLeastOneEntityField && ms.listeners?seq_contains("BROKER"))>
            <#items as messageSpec>
                <#assign specName>
                    <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${word?c_upper_case}<#sep>_</#list>
                </#assign>
            case ${retrieveApiKey(messageSpec)} -> inVersion(apiVersion, ${specName?trim}_VERSIONS);
            </#items>
        </#list>
            default -> false;
        };
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        // Find coordinator uses identifies entities using a key type
        if (apiKey == FIND_COORDINATOR) {
            return true;
        }
        return switch (apiKey) {
        <#list messageSpecs?filter(ms -> ms.type?lower_case == 'response' && ms.hasAtLeastOneEntityField && retrieveApiListener(ms)?seq_contains("BROKER"))>
            <#items as messageSpec>
                    <#assign specName>
                        <#assign words=messageSpec.name?split("(?=[A-Z])", "r")><#list words as word>${
                    word?c_upper_case}<#sep>_</#list>
                    </#assign>
            case ${retrieveApiKey(messageSpec)} -> inVersion(apiVersion, ${specName?trim}_VERSIONS);
            </#items>
        </#list>
        default -> false;
        };
    }

    private static boolean inVersion(short apiVersions, Set<Short> versions) {
        return versions.contains(apiVersions);
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
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
                    if (config.resourceTypes().contains(UserNamespace.ResourceType.GROUP_ID) &&  findCoordinatorRequestData.keyType() == 0 ) {
                        if (inVersion(header.requestApiVersion(), Set.of( (short) 0,  (short) 1,  (short) 2,  (short) 3))) {
                            findCoordinatorRequestData.setKey(map(aid, findCoordinatorRequestData.key()));
                        }
                        else {
                            findCoordinatorRequestData.setCoordinatorKeys(findCoordinatorRequestData.coordinatorKeys().stream().map(k -> map(aid, k)).toList());
                        }
                    }
                    LOGGER.atDebug().addArgument(findCoordinatorRequestData).log("find coordinator request result: {}");
                }
            }


            switch (apiKey) {
<#list messageSpecs?filter(ms -> ms.type?lower_case == 'request' && ms.hasAtLeastOneEntityField && ms.listeners?seq_contains("BROKER"))>
    <#items as messageSpec>
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
    </#items>
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
        if (originalName == null || originalName.isEmpty()) {
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

    private static boolean inNamespace(String authId, String mappedName) {
        return mappedName.startsWith(authId + "-");
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
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
<#list messageSpecs?filter(ms -> ms.type?lower_case == 'response' && ms.hasAtLeastOneEntityField && retrieveApiListener(ms)?seq_contains("BROKER"))>
    <#items as messageSpec>
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
                    <@mapAndFilterResponseFields messageSpec "" dataVar dataClass messageSpec.fields 5/>
                    LOGGER.atDebug()
                            .addArgument(context.sessionId())
                            .addArgument(aid)
                            .addArgument(${dataVar})
                            .log("{} for {}: response result ${specName?trim}: {}");
                }
    </#items>
</#list>
            }
        });
        return context.forwardResponse(header, response);
    }
}