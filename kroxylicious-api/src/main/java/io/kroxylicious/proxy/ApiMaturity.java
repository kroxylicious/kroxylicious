/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotations to specify the maturity of APIs.
 */
public class ApiMaturity {

    /**
     * No guarantee is provided as to reliability or stability across any level of release granularity.
     *
     * If an element is not annotated with an ApiMaturity annotation then the maturity is implicitly the maturity of
     * the enclosing declaration.
     */
    @Target({ ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.ANNOTATION_TYPE, ElementType.PACKAGE })
    @Retention(RetentionPolicy.SOURCE)
    @Documented
    @interface Unstable {
    }

    /**
     * Compatibility may be broken at minor release (i.e. m.x).
     *
     * If an element is not annotated with an ApiMaturity annotation then the maturity is implicitly the maturity of
     * the enclosing declaration.
     */
    @Target({ ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.ANNOTATION_TYPE, ElementType.PACKAGE })
    @Retention(RetentionPolicy.SOURCE)
    @Documented
    @interface Evolving {
    }

    /**
     * Compatibility is maintained in major, minor and patch releases with one exception: compatibility may be broken
     * in a major release (i.e. 0.m) for APIs that have been deprecated for at least one major/minor release cycle.
     *
     * If an element is not annotated with an ApiMaturity annotation then the maturity is implicitly the maturity of
     * the enclosing declaration.
     */
    @Target({ ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.ANNOTATION_TYPE, ElementType.PACKAGE })
    @Retention(RetentionPolicy.SOURCE)
    @Documented
    @interface Stable {
    }
}
