/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.maven;

import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import io.kroxylicious.krpccodegen.main.KrpcGenerator;

/**
 * A Maven plugin capable of generating java source from Apache Kafka message
 * specification definitions. This generator is invoked once per message specification.
 * The Apache FreeMarker variable {@code inputSpec} is defined with the message specification
 * being processed.
 */
@Mojo(name = "generate-single", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class KrpcSingleGeneratorMojo extends AbstractKrpcGeneratorMojo {

    /**
     * Constructs a single-generator.
     */
    public KrpcSingleGeneratorMojo() {
        super();
    }

    @Override
    protected KrpcGenerator.Builder builder() {
        return KrpcGenerator.single();
    }
}
