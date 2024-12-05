/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.util.List;

import com.github.javaparser.ast.expr.AnnotationExpr;

import io.kroxylicious.tools.schema.model.SchemaObject;

public interface TypeAnnotator {

    /**
     * Return annotations to be added to the class.
     * @param diagnostics Diagnostics for reporting errors.
     * @param typeSchema The schema of the type
     * @return The annotations to add
     */
    default List<AnnotationExpr> annotateClass(Diagnostics diagnostics, SchemaObject typeSchema) {
        return List.of();
    }
}
