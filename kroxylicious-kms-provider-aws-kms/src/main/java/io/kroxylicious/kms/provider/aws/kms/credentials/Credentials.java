/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.util.Optional;

public interface Credentials {
    String accessKey();

    String secretKey();

    default Optional<String> securityToken() {
        return Optional.empty();
    }
}
