/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to be applied to the {@code on*()} method of a KrpcFilter implementation to narrow the range of
 * API versions for which the filter will be called.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface ApiVersions {
    /**
     * @return The inclusive lower bound on the API version for which the filter will be called.
     * {@link #from()} must be be less than or equal to {@link #to()}.
     */
    short from() default -1;

    /**
     * @return The inclusive upper bound in the API version for which the filter will be called.
     * {@link #from()} must be be less than or equal to {@link #to()}.
     */
    short to() default -1;
}
