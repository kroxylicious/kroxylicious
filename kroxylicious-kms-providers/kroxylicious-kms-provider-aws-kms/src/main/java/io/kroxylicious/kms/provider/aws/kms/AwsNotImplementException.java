/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import io.kroxylicious.kms.service.KmsException;

/**
 * Thrown when a client tries to access to a function not implemented on AWS.
 */
public class AwsNotImplementException extends KmsException {

    public AwsNotImplementException(String alias) {
        super(alias);
    }
}
