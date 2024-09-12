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

/**
 * A Maven plugin capable of generating java source from Apache Kafka message
 * specifications definitions.  The generator is invoked once per message specification.
 * The Apache FreeMaker variable {@code messageSpec} is defined with the message specification
 * being processed.
 */
@Mojo(
        name = "generate-multi",
        defaultPhase = LifecyclePhase.GENERATE_SOURCES
)
public class KrpcMultiGeneratorMojo extends AbstractKrpcGeneratorMojo {

    /**
     * Constructs a multi-generator.
     */
    public KrpcMultiGeneratorMojo() {
        super();
    }

    @Override
    protected Builder builder() {
        return KrpcGenerator.multi();
    }
}
