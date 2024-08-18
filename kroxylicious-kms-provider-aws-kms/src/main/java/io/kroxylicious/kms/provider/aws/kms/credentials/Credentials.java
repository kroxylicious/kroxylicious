/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.util.Optional;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Representations credentials needed to authenticate to AWS.
 */
public interface Credentials {
    /**
     * Specifies an AWS access key associated with an IAM user or role.
     *
     * @return access key id.
     */
    @NonNull
    String accessKeyId();

    /**
     * Specifies the secret key associated with the access key.
     *
     * @return secret key.
     */
    @NonNull
    String secretAccessKey();

    /**
     * The temporary security token associated with the access key id.  This is present
     * only when the access key is temporary.
     *
     * @return security token.
     */
    @NonNull
    default Optional<String> securityToken() {
        return Optional.empty();
    }

}
