/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.nio.file.Path;

import io.kroxylicious.tools.schema.model.SchemaObject;

public record Input(
                    Path schemaPath,
                    String pkg,
                    SchemaObject rootSchema) {}
