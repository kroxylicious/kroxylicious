/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import io.kroxylicious.tools.schema.compiler.SchemaCompiler;

@Mojo(name = "compile-schema", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class CompileSchemaMojo extends AbstractCompileSchemaMojo {
    @Override
    protected SchemaCompiler schemaCompiler() throws MojoExecutionException {
        SchemaCompiler schemaCompiler = new SchemaCompiler(List.of(source.toPath()),
                null,
                readHeaderFile(),
                existingClasses != null ? existingClasses : Map.of(),
                List.of(),
                List.of());
        return schemaCompiler;
    }
}
