/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.dsl.ExecListener;

/**
 * The Kube listener.
 */
public class KubeSimpleListener implements ExecListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubeSimpleListener.class);

    @Override
    public void onOpen() {
        LOGGER.debug("shell open");
    }

    @Override
    public void onFailure(Throwable t, Response failureResponse) {
        LOGGER.error("shell closed with failure: " + failureResponse.code());
    }

    @Override
    public void onClose(int code, String reason) {
        LOGGER.debug("The shell will now close.");
    }
}
