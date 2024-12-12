/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import io.kroxylicious.systemtests.resources.operator.SetUpKroxyliciousOperator;

public class KroxyliciousOperator {
    private final String deploymentNamespace;
    private SetUpKroxyliciousOperator setUpKroxyliciousOperator;

    public KroxyliciousOperator(String deploymentNamespace) {
        this.deploymentNamespace = deploymentNamespace;
    }

    public void deploy() {
        setUpKroxyliciousOperator = new SetUpKroxyliciousOperator(deploymentNamespace);
        setUpKroxyliciousOperator.install();
    }

    public void delete() {
        setUpKroxyliciousOperator.delete();
    }
}
