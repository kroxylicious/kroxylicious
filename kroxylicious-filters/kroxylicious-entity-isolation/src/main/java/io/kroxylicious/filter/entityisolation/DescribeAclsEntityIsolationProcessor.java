/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * Entity isolation processor for DELETE_ACLS.
 * This implementation is handwritten as the fields of the RPC doesn't follow a common naming convention.
*/
class DescribeAclsEntityIsolationProcessor implements EntityIsolationProcessor<DescribeAclsRequestData, DescribeAclsResponseData, Void> {

    private final Predicate<EntityType> shouldMap;
    private final EntityNameMapper mapper;

    DescribeAclsEntityIsolationProcessor(Predicate<EntityType> shouldMap, EntityNameMapper mapper) {
        this.shouldMap = Objects.requireNonNull(shouldMap);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public short minSupportedVersion() {
        return 1;
    }

    @Override
    public short maxSupportedVersion() {
        return 3;
    }

    @Override
    public boolean shouldHandleRequest(short apiVersion) {
        return (short) 1 <= apiVersion && apiVersion <= (short) 3;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, short apiVersion, DescribeAclsRequestData request, FilterContext filterContext,
                                                          MapperContext mapperContext) {
        EntityIsolation.fromResourceTypeCode(ApiKeys.DELETE_ACLS, request.resourceTypeFilter())
                .filter(shouldMap)
                .ifPresent(rt -> request.setResourceNameFilter(mapper.map(mapperContext, rt, request.resourceNameFilter())));

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
        // process entity fields defined at this level
        // process the resource list
        if ((short) 1 <= apiVersion && apiVersion <= (short) 3 && response.resources() != null) {
            var resourcesIterator = response.resources().iterator();
            while (resourcesIterator.hasNext()) {
                var describeAclsResource = resourcesIterator.next();
                EntityIsolation.fromResourceTypeCode(ApiKeys.DESCRIBE_ACLS, describeAclsResource.resourceType())
                        .filter(shouldMap)
                        .ifPresent(entityType -> {
                            if (mapper.isInNamespace(mapperContext, entityType, describeAclsResource.resourceName())) {
                                describeAclsResource.setResourceName(mapper.unmap(mapperContext, entityType, describeAclsResource.resourceName()));
                            }
                            else {
                                resourcesIterator.remove();
                            }
                        });
            }
        }
        return filterContext.forwardResponse(header, response);
    }

}
