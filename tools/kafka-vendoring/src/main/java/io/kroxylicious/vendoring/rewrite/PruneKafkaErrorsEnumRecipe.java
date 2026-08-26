/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.vendoring.rewrite;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.RemoveUnusedImports;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.Statement;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Applies the required edits to Apache Kafka's hand rolled Errors enum to make it part of the Kroxylicious API
 */
public class PruneKafkaErrorsEnumRecipe extends Recipe {

    /**
     * Create an instance of the recipe.
     */
    public PruneKafkaErrorsEnumRecipe() {
        super();
    }

    @Override
    @NonNull
    public String getDisplayName() {
        return "Prune exception references from vendored Kafka Errors enum";
    }

    @Override
    @NonNull
    public String getDescription() {
        return "Strips client exception builders, fields, methods, and imports from Kafka Errors enum.";
    }

    @Override
    @NonNull
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaVisitor<>() {

            @Nullable
            @Override
            public J.EnumValue visitEnumValue(J.EnumValue enumConstant, ExecutionContext ctx) {
                J.EnumValue ec = (J.EnumValue) super.visitEnumValue(enumConstant, ctx);
                J.NewClass initializer = ec.getInitializer();
                if (initializer != null) {
                    List<Expression> arguments = initializer.getArguments();
                    if (arguments.size() == 3) {
                        List<Expression> replacementArgs = new ArrayList<>(arguments);
                        replacementArgs.remove(2); // Remove 3rd argument (Exception constructor reference)
                        return ec.withInitializer(initializer.withArguments(replacementArgs));
                    }
                }
                return ec;
            }

            @Override
            public J.VariableDeclarations visitVariableDeclarations(J.VariableDeclarations multiVariable, ExecutionContext ctx) {
                J.VariableDeclarations vd = (J.VariableDeclarations) super.visitVariableDeclarations(multiVariable, ctx);
                if (vd.getVariables().stream().anyMatch(v -> "builder".equals(v.getSimpleName()))) {
                    return null; // Remove 'builder' field
                }
                return vd;
            }

            @Override
            public J.MethodDeclaration visitMethodDeclaration(J.MethodDeclaration method, ExecutionContext ctx) {
                J.MethodDeclaration md = (J.MethodDeclaration) super.visitMethodDeclaration(method, ctx);
                String name = md.getSimpleName();

                // Remove exception factory methods
                if ("exception".equals(name) || "forException".equals(name)) {
                    return null;
                }

                // Update constructor signature and body
                if (md.isConstructor()) {
                    List<Statement> newParams = md.getParameters().stream()
                            .filter(p -> !(p instanceof J.VariableDeclarations vd &&
                                    vd.getVariables().stream().anyMatch(v -> "builder".equals(v.getSimpleName()))))
                            .collect(Collectors.toList());

                    J.Block body = md.getBody();
                    if (body != null) {
                        List<Statement> newStatements = body.getStatements().stream()
                                .filter(stmt -> !stmt.printTrimmed(getCursor()).contains("builder"))
                                .collect(Collectors.toList());
                        md = md.withBody(body.withStatements(newStatements));
                    }

                    return md.withParameters(newParams);
                }

                return md;
            }

            @Override
            public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                J.CompilationUnit c = (J.CompilationUnit) super.visitCompilationUnit(cu, ctx);
                doAfterVisit(new RemoveUnusedImports().getVisitor());
                return c;
            }
        };
    }
}
