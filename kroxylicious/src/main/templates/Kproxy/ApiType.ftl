<#--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.common.message.ApiMessageType;
<#list messageSpecs as messageSpec>
import org.apache.kafka.common.message.${messageSpec.name}Data;
</#list>
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
<#list messageSpecs as messageSpec>
import io.kroxylicious.proxy.filter.${messageSpec.name}Filter;
</#list>
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Enumerates the Kafka APIs, associating {@code ApiKeys}, {@code ApiMessageType}the subclasses of
 * {@link KrpcFilter} and the subclasses of {@code ApiMessage} that they consume.
 */
public enum ApiType {
    // In key order
<#assign prevFilterType=""/>
<#assign prevMessageType=""/>
<#list messageSpecs as messageSpec>
    ${retrieveApiKey(messageSpec)}_${messageSpec.type?upper_case}(
        ${messageSpec.name}Filter.class, "on${messageSpec.name}",
        ${messageSpec.name}Data.class,
        ApiKeys.${retrieveApiKey(messageSpec)},
        ApiMessageType.${retrieveApiKey(messageSpec)},
<#if prevFilterType?length != 0>
        ${prevFilterType}.base + 1 + ${prevMessageType}.highestSupportedVersion() - ${prevMessageType}.lowestSupportedVersion())<#else>
        0)</#if><#sep>,
<#assign prevFilterType="${retrieveApiKey(messageSpec)}_${messageSpec.type?upper_case}"/>
<#assign prevMessageType="ApiMessageType.${retrieveApiKey(messageSpec)}"/></#list>;

    public static final int NUM_APIS;
    static {
        var last = ApiType.values()[ApiType.values().length - 1];
        int numApis = last.base + 1 + last.messageType.highestSupportedVersion() - last.messageType.lowestSupportedVersion();
        NUM_APIS = numApis;
    }

    public final Class<? extends KrpcFilter> filterClass;
    private final String filterMethod;
    public final Class<? extends ApiMessage> messageClass;
    public final ApiKeys apiKey;
    public final ApiMessageType messageType;
    private final int base;

    private ApiType(Class<? extends KrpcFilter> filterClass,
                       String filterMethod,
                       Class<? extends ApiMessage> messageClass,
                       ApiKeys apiKey,
                       ApiMessageType messageType,
                       int base) {
        this.filterClass = filterClass;
        this.filterMethod = filterMethod;
        this.messageClass = messageClass;
        this.apiKey = apiKey;
        this.messageType = messageType;
        this.base = base;
    }

    /**
     * Does this enum element represent a request?
     * @return true iff this enum element represents a request.
     */
    public boolean isRequest() {
        return this.ordinal() % 2 == 0;
    }

    /**
     * Does this enum element represent a response?
     * @return true iff this enum element represents a response.
     */
    public boolean isResponse() {
        return this.ordinal() % 2 == 1;
    }

    /**
     * @param apiKey An API key.
     * @param request Whether to get the request or response filter type.
     * @return The request filter type for the given {@code apiKey}.
     */
    public static ApiType forKey(ApiKeys apiKey, boolean request) {
        Objects.requireNonNull(apiKey);
        switch (apiKey) {
<#list messageSpecs as messageSpec>
<#if messageSpec.type?lower_case == 'request'>
        case ${retrieveApiKey(messageSpec)}:
            return request ? ApiType.${retrieveApiKey(messageSpec)}_REQUEST : ApiType.${retrieveApiKey(messageSpec)}_RESPONSE;
</#if>
</#list>
            default:
                throw new IllegalStateException();
        }
    }

    public int index(int version) {
        if (version > messageType.highestSupportedVersion()
                || version < messageType.lowestSupportedVersion()) {
            throw new IllegalArgumentException();
        }
        return base + version - messageType.lowestSupportedVersion();
    }

    public <A extends Annotation> A annotations(Class<A> annotationClass,
                                                Class<? extends KrpcFilter> filterImplementation) {
        Objects.requireNonNull(annotationClass);
        Objects.requireNonNull(filterImplementation);
        Method method = null;
        try {
            method = filterImplementation.getMethod(filterMethod, messageClass, KrpcFilterContext.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return method.getAnnotation(annotationClass);
    }

}
