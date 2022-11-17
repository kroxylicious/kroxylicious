/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.kafkacluster.extension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to be used with {@link BrokerCluster} to specify that the cluster
 * should use SASL PLAIN authentication.
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface SaslPlainAuth {

    /**
     * @return The configured users, which must be non-empty.
     */
    UserPassword[] value();

    @interface UserPassword {
        /**
         * @return A user name.
         */
        String user();

        /**
         * @return A password.
         */
        String password();
    }
}
