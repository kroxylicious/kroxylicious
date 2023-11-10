/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import org.junit.jupiter.params.converter.ArgumentConversionException;
import org.junit.jupiter.params.converter.TypedArgumentConverter;

import io.kroxylicious.proxy.service.HostPort;

public class HostPortConverter extends TypedArgumentConverter<String, HostPort> {

    public HostPortConverter() {
        super(String.class, HostPort.class);
    }

    @Override
    protected HostPort convert(String source) throws ArgumentConversionException {
        return HostPort.parse(source);
    }
}
