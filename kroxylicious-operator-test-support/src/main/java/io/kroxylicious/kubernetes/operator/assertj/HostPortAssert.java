/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.IntegerAssert;
import org.assertj.core.api.StringAssert;

import io.kroxylicious.proxy.service.HostPort;

public class HostPortAssert extends AbstractObjectAssert<HostPortAssert, HostPort> {
    public HostPortAssert(HostPort hostPort) {
        super(hostPort, HostPortAssert.class);
    }

    public StringAssert host() {
        return new StringAssert(actual.host());
    }

    public IntegerAssert port() {
        return new IntegerAssert(actual.port());
    }
}
