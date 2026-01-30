<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign filteredEntityTypes = createEntityTypeSet("GROUP_ID", "TRANSACTIONAL_ID", "TOPIC_NAME")>

<#macro camelNameToSnakeName camelName>
    <#compress>
        <#local words=camelName?split("(?=[A-Z])", "r")/>
        <#list words as word>${word?c_lower_case}<#sep>_</#list>
    </#compress>
</#macro>

<#macro fieldVersionSet messageSpec versions>
Set.of(<#list messageSpec.validVersions.intersect(versions) as version> (short) ${version}<#sep>, </#list>)</#macro>

<#macro mapRequestFields messageSpec dataVar constStem fields indent>
    <#local pad = ""?left_pad(4*indent)/>
    <#list fields?filter(field -> filteredEntityTypes?seq_contains(field.entityType))>
${pad}// process any entity fields defined at this level
        <#items as field>
            <#local snakeFieldName>
                <@camelNameToSnakeName field.name/>
            </#local>
            <#local getter="${field.name?uncap_first}"
                    setter="set${field.name}"
                    fieldVersionsConst="${snakeFieldName?c_upper_case}_${constStem}" />
${pad}if (shouldMap(ResourceIsolation.ResourceType.${field.entityType}) && inVersion(header.requestApiVersion(), ${fieldVersionsConst}_${messageSpec.type}_VERSIONS)) {
            <#if field.type == 'string'>
${pad}    ${dataVar}.${setter}(map(mapperContext, ResourceIsolation.ResourceType.${field.entityType}, ${dataVar}.${getter}()));
            <#elseif field.type == '[]string'>
${pad}    ${dataVar}.${setter}(${dataVar}.${getter}().stream().map(orig -> map(mapperContext, ResourceIsolation.ResourceType.${field.entityType}, orig)).toList());
            </#if>
${pad}}
        </#items>
    </#list>
${pad}
    <#list fields?filter(field -> field.type.isArray && field.fields?size != 0) >
${pad}// recursively process sub-fields
        <#items as field>
            <#local snakeFieldName>
                <@camelNameToSnakeName field.name/>
            </#local>
            <#local getter="${field.name?uncap_first}()"
                    elementVar=field.type?remove_beginning("[]")?uncap_first
                    fieldVersionsConst="${snakeFieldName?c_upper_case}_${constStem}" />
${pad}if (inVersion(header.requestApiVersion(), ${fieldVersionsConst}_${messageSpec.type}_VERSIONS) && ${dataVar}.${getter} != null) {
${pad}    ${dataVar}.${getter}.forEach(${elementVar} -> {
                <@mapRequestFields messageSpec elementVar fieldVersionsConst field.fields indent + 1 />
${pad}    });
${pad}}
        </#items>
    </#list>
</#macro>

<#macro mapAndFilterResponseFields messageSpec collectionIterator dataVar dataClass constStem fields indent>
    <#local pad = ""?left_pad(4*indent)/>
    <#list fields?filter(field -> filteredEntityTypes?seq_contains(field.entityType))>
${pad}// process entity fields defined at this level
        <#items as field>
            <#local snakeFieldName>
                <@camelNameToSnakeName field.name/>
            </#local>
            <#local getter="${field.name?uncap_first}"
                    setter="set${field.name}"
                    fieldVersionsConst="${snakeFieldName?c_upper_case}_${constStem}" />
${pad}if (shouldMap(ResourceIsolation.ResourceType.${field.entityType}) && inVersion(apiVersion, ${fieldVersionsConst}_${messageSpec.type}_VERSIONS)) {
            <#if field.type == 'string'>
${pad}    if (inNamespace(mapperContext, ResourceIsolation.ResourceType.${field.entityType}, ${dataVar}.${getter}())) {
${pad}        ${dataVar}.${setter}(unmap(mapperContext, ResourceIsolation.ResourceType.${field.entityType}, ${dataVar}.${getter}()));
${pad}    }
                <#if collectionIterator?has_content>
${pad}    else {
${pad}        ${collectionIterator}.remove();
${pad}    }
                </#if>
            <#elseif field.type == '[]string'>
${pad}    ${dataVar}.${setter}(${dataVar}.${getter}().stream()
${pad}                        .filter(orig -> inNamespace(mapperContext, ResourceIsolation.ResourceType.${field.entityType}, orig))
${pad}                        .map(orig -> unmap(mapperContext, ResourceIsolation.ResourceType.${field.entityType}, orig)).toList());
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
            <#local snakeFieldName>
                <@camelNameToSnakeName field.name/>
            </#local>
            <#local getter="${field.name?uncap_first}"
                    collectionIteratorVar=field.name?uncap_first + "Iterator"
                    elementVar=field.type?remove_beginning("[]")?uncap_first
                    fieldVersionsConst="${snakeFieldName?c_upper_case}_${constStem}" />
${pad}if (inVersion(apiVersion, ${fieldVersionsConst}_${messageSpec.type}_VERSIONS) && ${dataVar}.${getter}() != null) {
${pad}    var ${collectionIteratorVar} = ${dataVar}.${getter}().iterator();
${pad}    while (${collectionIteratorVar}.hasNext()) {
${pad}       var ${elementVar} = ${collectionIteratorVar}.next();
            <@mapAndFilterResponseFields messageSpec collectionIteratorVar elementVar dataClass fieldVersionsConst field.fields indent + 2 />
${pad}    }
${pad}}
        </#items>
    </#list>
</#macro>

<#macro writeFieldConstants messageSpec stem fields>
    <#list fields?filter(field -> filteredEntityTypes?seq_contains(field.entityType))>
        <#items as field>
            <#local snakeFieldName>
                <@camelNameToSnakeName field.name/>
            </#local>
    private static final Set<Short> ${snakeFieldName?c_upper_case}_${stem}_${messageSpec.type}_VERSIONS = <@fieldVersionSet messageSpec field.versions/>;
        </#items>
    </#list>
    <#list fields?filter(field -> field.type.isArray && field.fields?size != 0) >
        <#items as field>
            <#local snakeFieldName>
                <@camelNameToSnakeName field.name/>
            </#local>
    private static final Set<Short> ${snakeFieldName?c_upper_case}_${stem}_${messageSpec.type}_VERSIONS = <@fieldVersionSet messageSpec field.versions/>;
            <@writeFieldConstants messageSpec "${snakeFieldName?c_upper_case}_${stem}" field.fields />
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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.annotation.processing.Generated;

<#list messageSpecs?filter(ms -> retrieveApiListener(ms)?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign messageSpecHasEntityFields=messageSpec.hasAtLeastOneEntityField(filteredEntityTypes) || retrieveApiKey(messageSpec) == "FIND_COORDINATOR"/>
        <#if (messageSpec.type == 'REQUEST' || messageSpec.type == 'RESPONSE') && messageSpecHasEntityFields>
import org.apache.kafka.common.message.${messageSpec.dataClassName};
        </#if>
    </#items>
</#list>

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
* Resource isolation filter.
* <p>Note: this class is automatically generated from a template</p>
*/
@Generated("io.kroxylicious.krpccodegen.main.KrpcGenerator")
class ResourceIsolationFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceIsolationFilter.class);

    <#list messageSpecs?filter(ms -> (ms.type == 'REQUEST' || ms.type == 'RESPONSE') && ms.hasAtLeastOneEntityField(filteredEntityTypes) && retrieveApiListener(ms)?seq_contains("BROKER"))>
        <#items as messageSpec>
            <#assign key=retrieveApiKey(messageSpec)/>
    // Constants for ${key}_${messageSpec.type}
    private static final Set<Short> ${key}_${messageSpec.type}_VERSIONS = Set.of(<#list messageSpec.intersectedVersionsForEntityFields(filteredEntityTypes) as version>(short) ${version}<#sep>, </#list>);
            <@writeFieldConstants messageSpec key messageSpec.fields/>

        </#items>
    </#list>

    private final Set<ResourceIsolation.ResourceType> resourceTypes;
    private final ResourceNameMapper mapper;

    ResourceIsolationFilter(Set<ResourceIsolation.ResourceType> resourceTypes, ResourceNameMapper mapper) {
        this.resourceTypes = Objects.requireNonNull(resourceTypes);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            // Find coordinator uses a key type to identify group entities, rather than entity field.
            case FIND_COORDINATOR -> true;
            // TODO: ALTER_CONFIG and INCREMENTAL_ALTER_CONFIG use a key type to identity topic entities.
            // TODO *_ACL use a  key type to identity topic/group/transactionalId entities.
        <#list messageSpecs?filter(ms -> ms.type == 'REQUEST' && ms.hasAtLeastOneEntityField(filteredEntityTypes) && ms.listeners?seq_contains("BROKER"))>
            <#items as messageSpec>
            case ${retrieveApiKey(messageSpec)} -> inVersion(apiVersion,  ${retrieveApiKey(messageSpec)}_REQUEST_VERSIONS);
            </#items>
        </#list>
            default -> false;
        };
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return switch (apiKey) {
            // Find coordinator uses a key type to identify entities, rather than entity field.
            case FIND_COORDINATOR -> true;
        <#list messageSpecs?filter(ms -> ms.type == 'RESPONSE' && ms.hasAtLeastOneEntityField(filteredEntityTypes) && retrieveApiListener(ms)?seq_contains("BROKER"))>
            <#items as messageSpec>
            case ${retrieveApiKey(messageSpec)} -> inVersion(apiVersion, ${retrieveApiKey(messageSpec)}_RESPONSE_VERSIONS);
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
        var mapperContext = buildMapperContext(context);
        switch (apiKey) {
            case FIND_COORDINATOR -> {
                FindCoordinatorRequestData findCoordinatorRequestData = (FindCoordinatorRequestData) request;
                log(context, "request", ApiKeys.FIND_COORDINATOR, findCoordinatorRequestData);
                if (resourceTypes.contains(ResourceIsolation.ResourceType.GROUP_ID) && findCoordinatorRequestData.keyType() == 0) {
                    if (inVersion(header.requestApiVersion(), Set.of( (short) 0,  (short) 1,  (short) 2,  (short) 3))) {
                        findCoordinatorRequestData.setKey(map(mapperContext, ResourceIsolation.ResourceType.GROUP_ID, findCoordinatorRequestData.key()));
                    }
                    else {
                        findCoordinatorRequestData.setCoordinatorKeys(findCoordinatorRequestData.coordinatorKeys().stream().map(k -> map(mapperContext, ResourceIsolation.ResourceType.GROUP_ID, k)).toList());
                    }
                }
                log(context, "request result", ApiKeys.FIND_COORDINATOR, findCoordinatorRequestData);
            }
<#list messageSpecs?filter(ms -> ms.type == 'REQUEST' && ms.hasAtLeastOneEntityField(filteredEntityTypes) && ms.listeners?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign key=retrieveApiKey(messageSpec)
                 dataClass="${messageSpec.dataClassName}"
                 dataVar="${messageSpec.dataClassName?uncap_first}"/>
            case ${key} -> {
                var ${dataVar} = (${dataClass}) request;
                log(context, "${messageSpec.type?c_lower_case}", ApiKeys.${key}, ${dataVar});
                <@mapRequestFields messageSpec dataVar key messageSpec.fields 4/>
                log(context, "${messageSpec.type?c_lower_case} result", ApiKeys.${key}, ${dataVar});
            }
    </#items>
</#list>
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        var mapperContext = buildMapperContext(context);
        switch (apiKey) {
            case FIND_COORDINATOR -> {
                FindCoordinatorResponseData findCoordinatorResponseData = (FindCoordinatorResponseData) response;
                log(context, "response", ApiKeys.FIND_COORDINATOR, findCoordinatorResponseData);
                findCoordinatorResponseData.coordinators().forEach(
                        coordinator -> coordinator.setKey(unmap(mapperContext, ResourceIsolation.ResourceType.GROUP_ID, coordinator.key())));
                log(context, "response result", ApiKeys.FIND_COORDINATOR, findCoordinatorResponseData);
            }
<#list messageSpecs?filter(ms -> ms.type == 'RESPONSE' && ms.hasAtLeastOneEntityField(filteredEntityTypes) && retrieveApiListener(ms)?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign key=retrieveApiKey(messageSpec)
                 dataClass="${messageSpec.dataClassName}"
                 dataVar="${messageSpec.dataClassName?uncap_first}"/>
            case ${key} -> {
                var ${dataVar} = (${dataClass}) response;
                log(context, "${messageSpec.type?c_lower_case}", ApiKeys.${key}, ${dataVar});
                <@mapAndFilterResponseFields messageSpec "" dataVar dataClass key messageSpec.fields 4/>
                log(context, "${messageSpec.type?c_lower_case} result", ApiKeys.${key}, ${dataVar});
            }
    </#items>
</#list>
        }
        return context.forwardResponse(header, response);
    }

    private boolean shouldMap(ResourceIsolation.ResourceType entityType) {
        return resourceTypes.contains(entityType);
    }

    private String map(MapperContext context, ResourceIsolation.ResourceType resourceType, String originalName) {
        if (originalName == null || originalName.isEmpty()) {
            return originalName;
        }
        return mapper.map(context, resourceType, originalName);
    }

    private String unmap(MapperContext context, ResourceIsolation.ResourceType resourceType, String mappedName) {
        if (mappedName.isEmpty()) {
            return mappedName;
        }
        return mapper.unmap(context, resourceType, mappedName);
    }

    private boolean inNamespace(MapperContext context, ResourceIsolation.ResourceType resourceType, String mappedName) {
        return mapper.isInNamespace(context, resourceType, mappedName);
    }

    private static MapperContext buildMapperContext(FilterContext context) {
        return new MapperContext(context.authenticatedSubject(),
                context.clientTlsContext().orElse(null),
                context.clientSaslContext().orElse(null));
    }

    private static void log(FilterContext context, String description, ApiKeys key, ApiMessage message) {
        LOGGER.atDebug()
                .addArgument(() -> context.sessionId())
                .addArgument(() -> context.authenticatedSubject())
                .addArgument(description)
                .addArgument(key)
                .addArgument(message)
                .log("{} for {}: {} {}: {}");
    }
}