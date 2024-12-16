/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import io.kroxylicious.systemtests.resources.operator.SetUpKroxyliciousOperator;

/**
 * The type Kroxylicious operator.
 */
public class KroxyliciousOperator {
    private final String deploymentNamespace;
    private SetUpKroxyliciousOperator setUpKroxyliciousOperator;

    /**
     * Instantiates a new Kroxylicious operator.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public KroxyliciousOperator(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
    }

    /**
     * Deploy.
     */
    public void deploy() {
        setUpKroxyliciousOperator = new SetUpKroxyliciousOperator(deploymentNamespace);
        setUpKroxyliciousOperator.install();
    }

    /**
     * Delete.
     */
    public void delete() {
        setUpKroxyliciousOperator.delete();
    }
}
