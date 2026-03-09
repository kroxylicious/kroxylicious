
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
package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * Entity isolation processor for DELETE_ACLS.
 * This implementation is handwritten as the fields of the RPC doesn't follow a common naming convention.
*/
class DescribeAclsEntityIsolationProcessor implements EntityIsolationProcessor<DescribeAclsRequestData, DescribeAclsResponseData, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeAclsEntityIsolationProcessor.class);

    private final Set<EntityIsolation.ResourceType> resourceTypes;
    private final EntityNameMapper mapper;

    DescribeAclsEntityIsolationProcessor(Set<EntityIsolation.ResourceType> resourceTypes, EntityNameMapper mapper) {
        this.resourceTypes = Objects.requireNonNull(resourceTypes);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public boolean shouldHandleRequest(short apiVersion) {
        return (short) 1 <= apiVersion && apiVersion <= (short) 3;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, short apiVersion, DescribeAclsRequestData request, FilterContext filterContext,
                                                          MapperContext mapperContext) {
        log(filterContext, "request", ApiKeys.DESCRIBE_ACLS, request);
        EntityIsolation.fromResourceTypeCode(ApiKeys.DELETE_ACLS, request.resourceTypeFilter())
                .filter(entityType -> shouldMap(entityType))
                .ifPresent(rt -> {
                    request.setResourceNameFilter(map(mapperContext, rt, request.resourceNameFilter()));
                });
        log(filterContext, "request result", ApiKeys.DESCRIBE_ACLS, request);

        return filterContext.forwardRequest(header, request);
    }

    @Override
    public boolean shouldHandleResponse(short apiVersion) {
        return (short) 1 <= apiVersion && apiVersion <= (short) 3;
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                            short apiVersion,
                                                            Void unusedRequestContext,
                                                            DescribeAclsResponseData response,
                                                            FilterContext filterContext,
                                                            MapperContext mapperContext) {
        log(filterContext, "response", ApiKeys.DESCRIBE_ACLS, response);
        // process entity fields defined at this level
        // process the resource list
        if ((short) 1 <= apiVersion && apiVersion <= (short) 3 && response.resources() != null) {
            var resourcesIterator = response.resources().iterator();
            while (resourcesIterator.hasNext()) {
                var describeAclsResource = resourcesIterator.next();
                EntityIsolation.fromResourceTypeCode(ApiKeys.DESCRIBE_ACLS, describeAclsResource.resourceType())
                        .filter(entityType -> shouldMap(entityType))
                        .ifPresent(entityType -> {
                            if (inNamespace(mapperContext, entityType, describeAclsResource.resourceName())) {
                                describeAclsResource.setResourceName(unmap(mapperContext, entityType, describeAclsResource.resourceName()));
                            }
                            else {
                                resourcesIterator.remove();
                            }
                        });
            }
        }
        log(filterContext, "response result", ApiKeys.DESCRIBE_ACLS, response);
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
