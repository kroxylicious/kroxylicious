/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.io.Closeable;
import java.util.concurrent.CompletionStage;

/**
 * Fortanix Session Provider
 */
public interface SessionProvider extends Closeable {

    /**
     * Gets a Fortanix Session
     *
     * @return future
     */
    CompletionStage<Session> getSession();

    default void close() {
    }
}
