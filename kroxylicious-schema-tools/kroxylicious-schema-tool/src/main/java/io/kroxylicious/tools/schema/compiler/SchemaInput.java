/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.nio.file.Path;

import io.kroxylicious.tools.schema.model.SchemaObject;

/**
 * An input to the {@link SchemaCompiler}.
 * @param schemaPath The path to the schema file
 * @param pkg The package that the schema is in
 * @param rootSchema The root schema in the given file.
 */
public record SchemaInput(
                          Path schemaPath,
                          String pkg,
                          SchemaObject rootSchema) {}
