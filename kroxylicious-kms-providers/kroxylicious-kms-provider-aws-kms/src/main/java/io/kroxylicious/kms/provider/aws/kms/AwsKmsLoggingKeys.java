/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

/**
 * Common keys for structured logging in kroxylicious-kms-provider-aws-kms.
 */
public class AwsKmsLoggingKeys {

    private AwsKmsLoggingKeys() {
    }
    
    /**
     * Credential.
     */
    public static final String CREDENTIAL = "credential";
    
    /**
     * Delay ms.
     */
    public static final String DELAY_MS = "delayMs";
    
    /**
     * Error.
     */
    public static final String ERROR = "error";
    
    /**
     * Expiration.
     */
    public static final String EXPIRATION = "expiration";
    
    /**
     * Iam role.
     */
    public static final String IAM_ROLE = "iamRole";
}
