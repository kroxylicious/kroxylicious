/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;

import io.kroxylicious.proxy.addressmapper.AddressManagerContributorManager;

public class AddressMapperConfigTypeIdResolver extends AbstractConfigTypeIdResolver {

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
        Class<?> subType = AddressManagerContributorManager.getInstance().getConfigType(id);
        return context.constructSpecializedType(superType, subType);
    }
}
