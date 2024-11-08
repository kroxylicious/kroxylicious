/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.Operator;

/**
 * The {@code main} method entrypoint for the operator
 */
public class OperatorMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorMain.class);

    public static void main(String[] args) {
        Operator operator = new Operator();
        operator.installShutdownHook(Duration.ofSeconds(10));
        operator.register(new ProxyReconciler(operator));
        operator.register(new FilterReconciler());
        try {
            operator.start();
            LOGGER.info("Operator started.");
        }
        catch (Exception e) {
            LOGGER.error("Operator has thrown exception during startup. Will now exit.", e);
            System.exit(1);
        }
    }
}
