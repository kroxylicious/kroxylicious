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
 * specifications definitions. This generator is invoked once per execution.
 * The Apache FreeMaker&#174; variable {@code messageSpecs} will be defined with a list containing
 * all message specifications.
 */
@Mojo(
        name = "generate-single",
        defaultPhase = LifecyclePhase.GENERATE_SOURCES
)
public class KrpcSingleGeneratorMojo extends AbstractKrpcGeneratorMojo {

    /**
     * Constructs a single-generator.
     */
    public KrpcSingleGeneratorMojo() {
        super();
    }

    @Override
    protected Builder builder() {
        return KrpcGenerator.single();
    }
}
