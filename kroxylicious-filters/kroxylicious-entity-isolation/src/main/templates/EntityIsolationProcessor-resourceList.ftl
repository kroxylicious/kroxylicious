<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
<#assign filteredEntityTypes = createEntityTypeSet("GROUP_ID", "TRANSACTIONAL_ID", "TOPIC_NAME")>

<#-- mapRequestFields - macro that generates mapping code for a request object using a resource list.

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
            <#if field.isResourceList>
        ${pad}// process the resource list
        ${pad}${fieldVar}.${getter}().stream()
        ${pad}            .collect(Collectors.toMap(Function.identity(),
        ${pad}                  r -> EntityIsolation.fromResourceTypeCode(ApiKeys.${inputSpec.apiKey}, r.resourceType())))
        ${pad}            .entrySet()
        ${pad}            .stream()
        ${pad}            .filter(e -> e.getValue().isPresent())
        ${pad}            .filter(e -> shouldMap.test(e.getValue().get())).forEach(e -> {
        ${pad}                e.getKey().setResourceName(mapper.map(mapperContext, e.getValue().get(), e.getKey().resourceName()));
        ${pad}    });
            </#if>
        </#items>
    </#list>
</#macro>

<#-- mapAndFilterResponseFields - macro that generates filtering and mapping code for a response object using a resource list.

messageSpec        message spec model object
collectionIterator collection iteration used to remove filtered items
fieldVar           name of the java variable refering to the current field
children           list of children directly beneath the current field
indent             java identation
-->
<#macro mapAndFilterResponseFields messageSpec collectionIterator fieldVar children indent>
    <#local pad = ""?left_pad(4*indent)/>
    <#list children>
        <#items as field>
            <#local getter="${field.name?uncap_first}" setter="set${field.name}" />
            <#if field.isResourceList>
${pad}// process the resource list
                <#local collectionIteratorVar=field.name?uncap_first + "Iterator"
                        elementVar=field.type?remove_beginning("[]")?uncap_first />
${pad}if (<@inVersionRange "apiVersion", messageSpec.validVersions.intersect(field.versions)/> && ${fieldVar}.${getter}() != null) {
${pad}    var ${collectionIteratorVar} = ${fieldVar}.${getter}().iterator();
${pad}    while (${collectionIteratorVar}.hasNext()) {
${pad}        var ${elementVar} = ${collectionIteratorVar}.next();
${pad}        EntityIsolation.fromResourceTypeCode(ApiKeys.${inputSpec.apiKey}, ${elementVar}.resourceType())
${pad}              .filter(shouldMap)
${pad}              .ifPresent(entityType -> {
${pad}            if (mapper.isInNamespace(mapperContext, entityType, ${elementVar}.resourceName())) {
${pad}                ${elementVar}.setResourceName(mapper.unmap(mapperContext, entityType, ${elementVar}.resourceName()));
${pad}            }
${pad}            else {
${pad}                ${collectionIteratorVar}.remove();
${pad}            }
${pad}        });
${pad}    }
${pad}}
            </#if>
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
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

<#if inputSpec.request.hasResourceList>
import java.util.function.Function;
import java.util.stream.Collectors;
</#if>

import javax.annotation.processing.Generated;

import org.apache.kafka.common.message.${inputSpec.response.dataClassName};
import org.apache.kafka.common.message.${inputSpec.request.dataClassName};

<#if inputSpec.request.hasResourceList>
import org.apache.kafka.common.message.RequestHeaderData;
</#if>
<#if inputSpec.response.hasResourceList>
import org.apache.kafka.common.message.ResponseHeaderData;
</#if>
import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;
import io.kroxylicious.proxy.filter.FilterContext;
<#if inputSpec.request.hasResourceList>
import io.kroxylicious.proxy.filter.RequestFilterResult;
</#if>
<#if inputSpec.response.hasResourceList>
import io.kroxylicious.proxy.filter.ResponseFilterResult;
</#if>

/**
* Entity isolation processor for ${inputSpec.apiKey}.
*/
@Generated(value = "io.kroxylicious.krpccodegen.main.KrpcGenerator", comments = "Generated by ${.template_name}")
class ${inputSpec.name}EntityIsolationProcessor implements EntityIsolationProcessor<${inputSpec.request.dataClassName}, ${inputSpec.response.dataClassName}, Void> {
    private final Predicate<EntityType> shouldMap;
    private final EntityNameMapper mapper;

    ${inputSpec.name}EntityIsolationProcessor(Predicate<EntityType> shouldMap, EntityNameMapper mapper) {
        this.shouldMap = Objects.requireNonNull(shouldMap);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public short minSupportedVersion() {
        return ${inputSpec.request.validVersions.lowest};
    }

    @Override
    public short maxSupportedVersion() {
        return ${inputSpec.request.validVersions.highest};
    }

<#if inputSpec.request.hasResourceList>
    <#assign key="${inputSpec.apiKey}" dataClass="${inputSpec.request.dataClassName}"/>
    @Override
    public boolean shouldHandleRequest(short apiVersion) {
        return <@inVersionRange "apiVersion" inputSpec.request.intersectedVersionsForResourceList()/>;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                          short apiVersion,
                                                          ${dataClass} request,
                                                          FilterContext filterContext,
                                                          MapperContext mapperContext) {
        <@mapRequestFields messageSpec=inputSpec.request fieldVar="request" children=inputSpec.request.fields indent=0/>
        return filterContext.forwardRequest(header, request);
    }
</#if>

<#if inputSpec.response.hasResourceList>
    <#assign key="${inputSpec.apiKey}" dataClass="${inputSpec.response.dataClassName}"/>
    @Override
    public boolean shouldHandleResponse(short apiVersion) {
        return <@inVersionRange "apiVersion" inputSpec.response.intersectedVersionsForResourceList()/>;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                            short apiVersion,
                                                            Void unusedRequestContext,
                                                            ${dataClass} response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        <@mapAndFilterResponseFields messageSpec=inputSpec.response
                                     collectionIterator=""
                                     fieldVar="response"
                                     children=inputSpec.response.fields
                                     indent=2/>
        return filterContext.forwardResponse(header, response);
    }
</#if>
}