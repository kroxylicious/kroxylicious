/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import io.kroxylicious.kms.service.KmsService;

/**
 * Represents the Kms itself, exposed for test purpose.
 */
public interface TestKmsFacade extends AutoCloseable {

    /**
     * Returns true of this facade is available, or false otherwise.
     * @return true if available, false otherwise.
     */
    default boolean isAvailable() {
        return true;
    }

    /**
     * Starts the underlying KMS.
     */
    void start();

    /**
     * Stops the underlying KMS.
     */
    void stop();

    /**
     * Gets a manager capable of managing the KEKs on the underlying KMS.
     * @return kek manager
     */
    TestKekManager getTestKekManager();

    /**
     * Gets the service class of used by Kroxylicious with the underlying KMS.
     *
     * @return service class
     */
    Class<? extends KmsService<?, ?, ?>> getKmsServiceClass();

    /**
     * Gets the configuration Kroxylicious will need to use to connect to the underlying KMS.
     * @return service configuration.
     */
    Object getKmsServiceConfig();

    default void close() {
        stop();
    }

}
