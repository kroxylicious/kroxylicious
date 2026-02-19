<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign filteredEntityTypes = createEntityTypeSet("GROUP_ID", "TRANSACTIONAL_ID", "TOPIC_NAME")>

<#-- mapRequestFields - recursive macro that generates mapping code for a request object.

messageSpec message spec model object
fieldVar    name of the java variable refering to the current field
children    list of children directly beneath the current field
indent      java identation
-->
<#macro mapRequestFields messageSpec fieldVar children indent>
    <#local pad = ""?left_pad(4*indent)/>
    <#list children>
        <#items as field>
            <#local getter="${field.name?uncap_first}" setter="set${field.name}" />
            <#if filteredEntityTypes?seq_contains(field.entityType)>
        ${pad}if (shouldMap(EntityIsolation.ResourceType.${field.entityType}) && <@inVersionRange "header.requestApiVersion()", messageSpec.validVersions.intersect(field.versions)/>) {
                <#if field.type == 'string'>
            ${pad}    ${fieldVar}.${setter}(map(mapperContext, EntityIsolation.ResourceType.${field.entityType}, ${fieldVar}.${getter}()));
                <#elseif field.type == '[]string'>
            ${pad}    ${fieldVar}.${setter}(${fieldVar}.${getter}().stream().map(orig -> map(mapperContext, EntityIsolation.ResourceType.${field.entityType}, orig)).toList());
                <#else>
                    <#stop "unexpected field type">
                </#if>
            ${pad}}
            <#elseif field.isResourceList>
        ${pad}// process the resource list
        ${pad}${fieldVar}.${getter}().stream()
        ${pad}            .collect(Collectors.toMap(Function.identity(),
        ${pad}                  r -> EntityIsolation.fromConfigResourceTypeCode(r.resourceType())))
        ${pad}            .entrySet()
        ${pad}            .stream()
        ${pad}            .filter(e -> e.getValue().isPresent())
        ${pad}            .filter(e -> shouldMap(e.getValue().get())).forEach(e -> {
        ${pad}                e.getKey().setResourceName(map(mapperContext, e.getValue().get(), e.getKey().resourceName()));
        ${pad}    });
            </#if>
        </#items>
    </#list>
    <#list children?filter(field -> field.type.isArray && field.fields?size != 0 && field.hasAtLeastOneEntityField(filteredEntityTypes)) >
${pad}// recursively process sub-fields
        <#items as childField>
            <#local getter="${childField.name?uncap_first}" elementVar=childField.type?remove_beginning("[]")?uncap_first />
${pad}if (<@inVersionRange "header.requestApiVersion()", messageSpec.validVersions.intersect(childField.versions)/> && ${fieldVar}.${getter}() != null) {
${pad}    ${fieldVar}.${getter}().forEach(${elementVar} -> {
                <@mapRequestFields messageSpec elementVar childField.fields indent + 2 />
        ${pad}    });
        ${pad}}
        </#items>
    </#list>
</#macro>

<#-- mapAndFilterResponseFields - recursive macro that generates filtering and mapping code for a response object.

messageSpec        message spec model object
collectionIterator collection iteration used to remove filtered items
fieldVar           name of the java variable refering to the current field
children           list of children directly beneath the current field
indent             java identation
-->
<#macro mapAndFilterResponseFields messageSpec collectionIterator fieldVar children indent>
    <#local pad = ""?left_pad(4*indent)/>
    <#list children?filter(field -> filteredEntityTypes?seq_contains(field.entityType))>
${pad}// process entity fields defined at this level
        <#items as field>
            <#local getter="${field.name?uncap_first}"
                    setter="set${field.name}" />
${pad}if (shouldMap(EntityIsolation.ResourceType.${field.entityType}) && <@inVersionRange "apiVersion", messageSpec.validVersions.intersect(field.versions)/> && ${fieldVar}.${getter}() != null) {
            <#if field.type == 'string'>
${pad}    if (inNamespace(mapperContext, EntityIsolation.ResourceType.${field.entityType}, ${fieldVar}.${getter}())) {
${pad}        ${fieldVar}.${setter}(unmap(mapperContext, EntityIsolation.ResourceType.${field.entityType}, ${fieldVar}.${getter}()));
${pad}    }
                <#if collectionIterator?has_content>
${pad}    else {
${pad}        ${collectionIterator}.remove();
${pad}    }
                </#if>
            <#elseif field.type == '[]string'>
${pad}    ${fieldVar}.${setter}(${fieldVar}.${getter}().stream()
${pad}                        .filter(orig -> inNamespace(mapperContext, EntityIsolation.ResourceType.${field.entityType}, orig))
${pad}                        .map(orig -> unmap(mapperContext, EntityIsolation.ResourceType.${field.entityType}, orig)).toList());
                <#if collectionIterator?has_content>
${pad}    if (!${fieldVar}.${getter}().isEmpty()) {
${pad}        ${collectionIterator}.remove();
${pad}    }
                </#if>
            <#else>
                <#stop "unexpected field type">
            </#if>
${pad}}
        </#items>
    </#list>
    <#list children?filter(field -> field.type.isArray && field.fields?size != 0 && field.hasAtLeastOneEntityField(filteredEntityTypes)) >
${pad}// recursively process sub-fields
        <#items as field>
            <#local getter="${field.name?uncap_first}"
                    collectionIteratorVar=field.name?uncap_first + "Iterator"
                    elementVar=field.type?remove_beginning("[]")?uncap_first />
${pad}if (<@inVersionRange "apiVersion", messageSpec.validVersions.intersect(field.versions)/> && ${fieldVar}.${getter}() != null) {
${pad}    var ${collectionIteratorVar} = ${fieldVar}.${getter}().iterator();
${pad}    while (${collectionIteratorVar}.hasNext()) {
${pad}       var ${elementVar} = ${collectionIteratorVar}.next();
            <@mapAndFilterResponseFields messageSpec collectionIteratorVar elementVar field.fields indent + 2 />
${pad}    }
${pad}}
        </#items>
    </#list>
</#macro>

<#-- inVersionRange - generates java code expressing a version predicate

varName name of java containing the version
versions ordered version number sequence
-->
<#macro inVersionRange varName versions>
<#compress>
<#local
minVersion = versions[0]
maxVersion = versions[versions?size - 1] >
<#if minVersion == maxVersion>(short) ${minVersion} == ${varName}<#else>(short) ${minVersion} <= ${varName} && ${varName} <= (short) ${maxVersion}</#if>
</#compress>
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
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.processing.Generated;

<#list messageSpecs?filter(ms -> retrieveApiListener(ms)?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign messageSpecHasEntityFields=messageSpec.hasAtLeastOneEntityField(filteredEntityTypes) || retrieveApiKey(messageSpec) == "FIND_COORDINATOR"/>
        <#assign messageSpecHasResourceList=messageSpec.hasResourceList/>
        <#if (messageSpec.type == 'REQUEST' || messageSpec.type == 'RESPONSE') && (messageSpecHasEntityFields || messageSpecHasResourceList)>
import org.apache.kafka.common.message.${messageSpec.dataClassName};
        </#if>
    </#items>
</#list>

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
* Entity isolation filter.
* <p>Note: this class is automatically generated from a template</p>
*/
@Generated(value = "io.kroxylicious.krpccodegen.main.KrpcGenerator", comments = "Generated by ${.template_name}")
class EntityIsolationFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntityIsolationFilter.class);

    private final Set<EntityIsolation.ResourceType> resourceTypes;
    private final EntityNameMapper mapper;

    EntityIsolationFilter(Set<EntityIsolation.ResourceType> resourceTypes, EntityNameMapper mapper) {
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
            // TODO LIST_TRANSACTIONS v2 has the ability to filter the returned transctions via a regexp, this needs custom code.
        <#list messageSpecs?filter(ms -> ms.type == 'REQUEST' && ms.listeners?seq_contains("BROKER"))>
            <#items as messageSpec>
                <#if messageSpec.hasAtLeastOneEntityField(filteredEntityTypes)>
            case ${retrieveApiKey(messageSpec)} -> <@inVersionRange "apiVersion" messageSpec.intersectedVersionsForEntityFields(filteredEntityTypes)/>;
                <#elseif messageSpec.hasResourceList>
            case ${retrieveApiKey(messageSpec)} -> <@inVersionRange "apiVersion" messageSpec.intersectedVersionsForResourceList()/>;
                </#if>
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
        <#list messageSpecs?filter(ms -> ms.type == 'RESPONSE' && retrieveApiListener(ms)?seq_contains("BROKER"))>
            <#items as messageSpec>
                <#if messageSpec.hasAtLeastOneEntityField(filteredEntityTypes)>
            case ${retrieveApiKey(messageSpec)} -> <@inVersionRange "apiVersion" messageSpec.intersectedVersionsForEntityFields(filteredEntityTypes)/>;
                <#elseif messageSpec.hasResourceList>
            case ${retrieveApiKey(messageSpec)} -> <@inVersionRange "apiVersion" messageSpec.intersectedVersionsForResourceList()/>;
                </#if>
            </#items>
        </#list>
        default -> false;
        };
    }

    private void onFindCoordinatorRequest(RequestHeaderData header,
                                          FindCoordinatorRequestData findCoordinatorRequestData,
                                          FilterContext filterContext,
                                          MapperContext mapperContext) {
        log(filterContext, "request", ApiKeys.FIND_COORDINATOR, findCoordinatorRequestData);
        // TODO handle FindCoordinatorRequest.CoordinatorType.TRANSACTION
        // TODO handle FindCoordinatorRequest.CoordinatorType.SHARE which uses a key in the format "groupId:topicId:partition"
        // TODO the response does not include the coordinator type, so we'll need to cache the request like the authorization filter does,\
        if (resourceTypes.contains(EntityIsolation.ResourceType.GROUP_ID) && findCoordinatorRequestData.keyType() ==  FindCoordinatorRequest.CoordinatorType.GROUP.id()) {

            if ((short) 0 <= header.requestApiVersion() && header.requestApiVersion() <= (short) 3) {
                findCoordinatorRequestData.setKey(map(mapperContext, EntityIsolation.ResourceType.GROUP_ID, findCoordinatorRequestData.key()));
            }
            else {
                findCoordinatorRequestData.setCoordinatorKeys(findCoordinatorRequestData.coordinatorKeys().stream().map(k -> map(mapperContext, EntityIsolation.ResourceType.GROUP_ID, k)).toList());
            }
        }
        log(filterContext, "request result", ApiKeys.FIND_COORDINATOR, findCoordinatorRequestData);
    }

<#list messageSpecs?filter(ms -> ms.type == 'REQUEST' && (ms.hasAtLeastOneEntityField(filteredEntityTypes) || ms.hasResourceList) && ms.listeners?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign key=retrieveApiKey(messageSpec)
        dataClass="${messageSpec.dataClassName}"
        namePad=""?left_pad(messageSpec.name?length) />
    private void on${messageSpec.name}(RequestHeaderData header,
                    ${namePad}${dataClass} request,
                    ${namePad}FilterContext filterContext,
                    ${namePad}MapperContext mapperContext) {
        log(filterContext, "${messageSpec.type?c_lower_case}", ApiKeys.${key}, request);
    <@mapRequestFields messageSpec=messageSpec fieldVar="request" children=messageSpec.fields indent=0/>
        log(filterContext, "${messageSpec.type?c_lower_case} result", ApiKeys.${key}, request);
    }
    </#items>
</#list>

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext filterContext) {
        var mapperContext = buildMapperContext(filterContext);
        switch (apiKey) {
            case FIND_COORDINATOR -> onFindCoordinatorRequest(header,
                        (FindCoordinatorRequestData) request,
                        filterContext,
                        mapperContext);
<#list messageSpecs?filter(ms -> ms.type == 'REQUEST' && (ms.hasAtLeastOneEntityField(filteredEntityTypes) || ms.hasResourceList) && ms.listeners?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign key=retrieveApiKey(messageSpec)
        dataClass="${messageSpec.dataClassName}" />
            case ${key} -> on${messageSpec.name}(header,
                        (${dataClass}) request,
                        filterContext,
                        mapperContext);
    </#items>
</#list>
        }
        return filterContext.forwardRequest(header, request);
    }

    private void onFindCoordinatorResponse(short apiVersion,
                                           ResponseHeaderData header,
                                           FindCoordinatorResponseData response,
                                           FilterContext context,
                                           MapperContext mapperContext) {
        log(context, "response", ApiKeys.FIND_COORDINATOR, response);
        response.coordinators().forEach(
                coordinator -> coordinator.setKey(unmap(mapperContext, EntityIsolation.ResourceType.GROUP_ID, coordinator.key())));
        log(context, "response result", ApiKeys.FIND_COORDINATOR, response);
    }

<#list messageSpecs?filter(ms -> ms.type == 'RESPONSE' && ms.hasAtLeastOneEntityField(filteredEntityTypes) && retrieveApiListener(ms)?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign key=retrieveApiKey(messageSpec)
        dataClass="${messageSpec.dataClassName}"
        namePad=""?left_pad(messageSpec.name?length) />
    private void on${messageSpec.name}(short apiVersion,
                    ${namePad}ResponseHeaderData header,
                    ${namePad}${messageSpec.dataClassName} response,
                    ${namePad}FilterContext filterContext,
                    ${namePad}MapperContext mapperContext) {
        log(filterContext, "${messageSpec.type?c_lower_case}", ApiKeys.${key}, response);
        <@mapAndFilterResponseFields messageSpec=messageSpec
                                     collectionIterator=""
                                     fieldVar="response"
                                     children=messageSpec.fields
                                     indent=2/>
        log(filterContext, "${messageSpec.type?c_lower_case} result", ApiKeys.${key}, response);
    }

    </#items>
</#list>

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext filterContext) {
        var mapperContext = buildMapperContext(filterContext);
        switch (apiKey) {
            case FIND_COORDINATOR -> onFindCoordinatorResponse(apiVersion,
                        header,
                        (FindCoordinatorResponseData) response,
                        filterContext,
                        mapperContext);
<#list messageSpecs?filter(ms -> ms.type == 'RESPONSE' && ms.hasAtLeastOneEntityField(filteredEntityTypes) && retrieveApiListener(ms)?seq_contains("BROKER"))>
    <#items as messageSpec>
        <#assign key=retrieveApiKey(messageSpec)
                 dataClass="${messageSpec.dataClassName}"
                 dataVar="${messageSpec.dataClassName?uncap_first}" />
            case ${key} -> on${messageSpec.name}(apiVersion,
                 header,
                 (${dataClass}) response,
                 filterContext,
                 mapperContext);
    </#items>
</#list>
        }
        return filterContext.forwardResponse(header, response);
    }

    private boolean shouldMap(EntityIsolation.ResourceType entityType) {
        return resourceTypes.contains(entityType);
    }

    private String map(MapperContext context, EntityIsolation.ResourceType resourceType, String originalName) {
        if (originalName == null || originalName.isEmpty()) {
            return originalName;
        }
        return mapper.map(context, resourceType, originalName);
    }

    private String unmap(MapperContext context, EntityIsolation.ResourceType resourceType, String mappedName) {
        if (mappedName.isEmpty()) {
            return mappedName;
        }
        return mapper.unmap(context, resourceType, mappedName);
    }

    private boolean inNamespace(MapperContext context, EntityIsolation.ResourceType resourceType, String mappedName) {
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