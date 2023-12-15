/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.listeners;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.CompletableFuture;

import io.fabric8.kubernetes.client.dsl.ExecListener;

public class SimpleListener implements ExecListener {

    private final CompletableFuture<String> data;
    private final ByteArrayOutputStream baos;

    public SimpleListener(CompletableFuture<String> data, ByteArrayOutputStream baos) {
        this.data = data;
        this.baos = baos;
    }

    @Override
    public void onOpen() {
        System.out.println("Reading data... ");
    }

    @Override
    public void onFailure(Throwable t, Response failureResponse) {
        System.err.println(t.getMessage());
        data.completeExceptionally(t);
    }

    @Override
    public void onClose(int code, String reason) {
        System.out.println("Exit with: " + code + " and with reason: " + reason);
        data.complete(baos.toString());
    }
}
