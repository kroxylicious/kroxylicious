/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

import io.kroxylicious.proxy.service.FilterFactoryManager;

public class FilterConfigTypeIdResolver extends TypeIdResolverBase {
    public static final Object FFM = new Object();
    private JavaType superType;

    @Override
    public void init(JavaType baseType) {
        superType = baseType;
    }

    @Override
    public String idFromValue(Object value) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String idFromValueAndType(Object value, Class<?> suggestedType) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public JsonTypeInfo.Id getMechanism() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
        FilterFactoryManager ffm = (FilterFactoryManager) context.getAttribute(FFM);
        Class<?> subType = ffm.getConfigType(id);
        return context.constructSpecializedType(superType, subType);
    }
}