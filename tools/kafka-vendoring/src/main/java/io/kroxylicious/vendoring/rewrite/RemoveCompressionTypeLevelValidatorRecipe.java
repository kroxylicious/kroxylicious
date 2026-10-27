/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.vendoring.rewrite;

import org.jspecify.annotations.NullMarked;
import org.openrewrite.ExecutionContext;
import org.openrewrite.FindSourceFiles;
import org.openrewrite.Preconditions;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.RemoveUnusedImports;
import org.openrewrite.java.tree.J;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Removes the levelValidator() method from Kafka's CompressionType enum.
 * This is a config-machinery method unneeded by Kroxylicious.
 */
public class RemoveCompressionTypeLevelValidatorRecipe extends Recipe {

    /**
     * Create an instance of the recipe.
     */
    public RemoveCompressionTypeLevelValidatorRecipe() {
        super();
    }

    @Override
    @NonNull
    public String getDisplayName() {
        return "Remove CompressionType.levelValidator";
    }

    @Override
    @NonNull
    public String getDescription() {
        return "Removes the levelValidator() method and its enum-constant overrides from CompressionType.";
    }

    @Override
    @NonNull
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return Preconditions.check(
                new FindSourceFiles("**/CompressionType.java"),
                new LevelValidatorVisitor());
    }

    @NullMarked
    private static class LevelValidatorVisitor extends JavaVisitor<ExecutionContext> {

        @Override
        public @Nullable J.MethodDeclaration visitMethodDeclaration(
                                                                    @NonNull J.MethodDeclaration methodDeclaration,
                                                                    @NonNull ExecutionContext ctx) {

            if ("levelValidator".equals(methodDeclaration.getSimpleName())) {
                return null;
            }
            return (J.MethodDeclaration) super.visitMethodDeclaration(methodDeclaration, ctx);
        }

        @Override
        public @NonNull J.CompilationUnit visitCompilationUnit(
                                                               @NonNull J.CompilationUnit cu,
                                                               @NonNull ExecutionContext ctx) {
            J.CompilationUnit c = (J.CompilationUnit) super.visitCompilationUnit(cu, ctx);
            doAfterVisit(new RemoveUnusedImports().getVisitor());
            return c;
        }
    }
}
