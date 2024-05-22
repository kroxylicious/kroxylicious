/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.function.Consumer;

public final class FlakyConfig {
    private final String initializeExceptionMsg;
    private final String createExceptionMsg;
    private final String closeExceptionMsg;

    private final Consumer<FlakyConfig> closeOrder;
    private final Consumer<FlakyConfig> initializeOrder;

    public FlakyConfig(String initializeExceptionMsg,
                       String createExceptionMsg,
                       String closeExceptionMsg,
                       Consumer<FlakyConfig> initializeOrder,
                       Consumer<FlakyConfig> closeOrder) {
        this.initializeExceptionMsg = initializeExceptionMsg;
        this.createExceptionMsg = createExceptionMsg;
        this.closeExceptionMsg = closeExceptionMsg;
        this.initializeOrder = initializeOrder;
        this.closeOrder = closeOrder;
    }

    public String initializeExceptionMsg() {
        return initializeExceptionMsg;
    }

    public String createExceptionMsg() {
        return createExceptionMsg;
    }

    public String closeExceptionMsg() {
        return closeExceptionMsg;
    }

    public void onClose() {
        closeOrder.accept(this);
    }

    public void onInitialize() {
        initializeOrder.accept(this);
    }
}
