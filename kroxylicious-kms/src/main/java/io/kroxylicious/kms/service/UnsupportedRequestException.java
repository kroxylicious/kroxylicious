/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * Thrown when a http request fails.
 */
public class UnsupportedRequestException extends KmsException {

    public UnsupportedRequestException(String alias) {
        super(alias);
    }
}