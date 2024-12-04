/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.util.List;

import com.github.javaparser.ast.expr.AnnotationExpr;

import io.kroxylicious.tools.schema.model.SchemaObject;

public interface PropertyAnnotator {

    /**
     * Return annotations to be added to the property field.
     * @param diagnostics Diagnostics for reporting errors.
     * @param property The name of the property.
     * @param propertySchema The schema of the property.
     * @return The annotations to add.
     */
    default List<AnnotationExpr> annotateField(Diagnostics diagnostics, String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property constructor parameter.
     * @param diagnostics Diagnostics for reporting errors.
     * @param property The name of the property.
     * @param propertySchema The schema of the property.
     * @return The annotations to add.
     */
    default List<AnnotationExpr> annotateConstructorParameter(Diagnostics diagnostics, String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property accessor method ("getter", but it might not be named {@code get*()}).
     * @param diagnostics Diagnostics for reporting errors.
     * @param property The name of the property.
     * @param propertySchema The schema of the property.
     * @return The annotations to add.
     */
    default List<AnnotationExpr> annotateAccessor(Diagnostics diagnostics, String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property mutator method ("setter", but it might not be named {@code set*()}).
     * @param diagnostics Diagnostics for reporting errors.
     * @param property The name of the property.
     * @param propertySchema The schema of the property.
     * @return The annotations to add.
     */
    default List<AnnotationExpr> annotateMutator(Diagnostics diagnostics, String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property mutator method's parameter.
     * @param diagnostics Diagnostics for reporting errors.
     * @param property The name of the property.
     * @param propertySchema The schema of the property.
     * @return The annotations to add.
     */
    default List<AnnotationExpr> annotateMutatorParameter(Diagnostics diagnostics, String property, SchemaObject propertySchema) {
        return List.of();
    }
}
