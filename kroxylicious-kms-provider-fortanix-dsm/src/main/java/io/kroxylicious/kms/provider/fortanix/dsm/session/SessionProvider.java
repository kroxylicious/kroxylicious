/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.io.Closeable;
import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Fortanix Session Provider
 */
public interface SessionProvider extends Closeable {

    /**
     * Gets a Fortanix Session
     *
     * @return future
     */
    @NonNull
    CompletionStage<Session> getSession();

    default void close() {
    }
}
