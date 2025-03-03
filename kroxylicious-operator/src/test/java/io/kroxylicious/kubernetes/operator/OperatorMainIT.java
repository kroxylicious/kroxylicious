/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class OperatorMainIT {
    // This is an IT because it depends on having a running Kube cluster

    @Test
    void run() {
        Assumptions.assumeThat(OperatorTestUtils.isKubeClientAvailable()).describedAs("Test requires a viable kube client").isTrue();
        OperatorMain.run();
    }

}