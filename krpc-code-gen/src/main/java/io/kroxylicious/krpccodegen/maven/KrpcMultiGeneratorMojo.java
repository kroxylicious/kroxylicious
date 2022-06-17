/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.maven;

import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import io.kroxylicious.krpccodegen.main.KrpcGenerator;
import io.kroxylicious.krpccodegen.main.KrpcGenerator.Builder;

@Mojo(name = "generate-multi", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class KrpcMultiGeneratorMojo extends AbstractKrpcGeneratorMojo {

    @Override
    protected Builder builder() {
        return KrpcGenerator.multi();
    }
}
