/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;

class OperatorMainIT {
    // This is an IT because it depends on having a running Kube cluster

    @Test
    void run() {
        OperatorMain.run();
    }

}