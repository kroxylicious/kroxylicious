/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 * The version of the wrapper schema used to persist information in the wrapper.
 */
public enum WrapperVersion {
    /** Version 1, used by pre-release versions of the filter. No longer supported. */
    V1_UNSUPPORTED,

    /** Version 2 of the wrapper schema. */
    V2;

}
