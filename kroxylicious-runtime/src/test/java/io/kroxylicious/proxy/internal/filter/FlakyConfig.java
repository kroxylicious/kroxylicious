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
    private final Consumer<FlakyConfig> onClose;
    private final Consumer<FlakyConfig> onInitialize;

    public FlakyConfig(
            String initializeExceptionMsg,
            String createExceptionMsg,
            String closeExceptionMsg,
            Consumer<FlakyConfig> onInitialize,
            Consumer<FlakyConfig> closeOrder
    ) {
        this.initializeExceptionMsg = initializeExceptionMsg;
        this.createExceptionMsg = createExceptionMsg;
        this.closeExceptionMsg = closeExceptionMsg;
        this.onInitialize = onInitialize;
        this.onClose = closeOrder;
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
        onClose.accept(this);
    }

    public void onInitialize() {
        onInitialize.accept(this);
    }
}
