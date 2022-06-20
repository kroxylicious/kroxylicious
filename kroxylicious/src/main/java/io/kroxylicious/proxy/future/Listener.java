/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.future;

interface Listener<T> {

    /**
     * Signal the success.
     *
     * @param value the value
     */
    void onSuccess(T value);

    /**
     * Signal the failure
     *
     * @param failure the failure
     */
    void onFailure(Throwable failure);
}
