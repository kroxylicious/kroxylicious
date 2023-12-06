/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import io.kroxylicious.kms.service.KmsService;

public interface TestKmsFacade extends AutoCloseable {

    void start();

    void stop();

    TestKekManager getTestKekManager();

    Class<? extends KmsService<?, ?, ?>> getKmsServiceClass();

    Object getKmsServiceConfig();

    default void close() {
        stop();
    }

}
