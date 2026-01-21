/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.usernamespace;

import io.kroxylicious.filter.usernamespace.UserNamespace.ResourceType;

public interface ResourceNameMapper {
    String map(MapperContext mapperContext, ResourceType resourceType,
               String unmappedResourceName);

    String unmap(MapperContext mapperContext, ResourceType resourceType,
                 String mappedResourceName);

    boolean isInNamespace(MapperContext mapperContext, ResourceType resourceType, String mappedResourceName);

}
