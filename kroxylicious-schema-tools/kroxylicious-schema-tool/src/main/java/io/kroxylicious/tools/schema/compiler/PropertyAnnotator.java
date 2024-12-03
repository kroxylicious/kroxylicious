/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import com.github.javaparser.ast.expr.AnnotationExpr;

import io.kroxylicious.tools.schema.model.SchemaObject;

import java.util.List;

public interface PropertyAnnotator {

    /**
     * Return annotations to be added to the property field.
     * @param property The name of the property
     * @param propertySchema The schema of the property
     * @return The annotations to add
     */
    default List<AnnotationExpr> annotateField(String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property constructor parameter.
     * @param property The name of the property
     * @param propertySchema The schema of the property
     * @return The annotations to add
     */
    default List<AnnotationExpr> annotateConstructorParameter(String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property accessor method ("getter", but it might not be named {@code get*()}).
     * @param property The name of the property
     * @param propertySchema The schema of the property
     * @return The annotations to add
     */
    default List<AnnotationExpr> annotateAccessor(String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property mutator method ("setter", but it might not be named {@code set*()}).
     * @param property The name of the property
     * @param propertySchema The schema of the property
     * @return The annotations to add
     */
    default List<AnnotationExpr> annotateMutator(String property, SchemaObject propertySchema) {
        return List.of();
    }

    /**
     * Return annotations to be added to the property mutator method's parameter.
     * @param property The name of the property
     * @param propertySchema The schema of the property
     * @return The annotations to add
     */
    default List<AnnotationExpr> annotateMutatorParameter(String property, SchemaObject propertySchema) {
        return List.of();
    }
}
