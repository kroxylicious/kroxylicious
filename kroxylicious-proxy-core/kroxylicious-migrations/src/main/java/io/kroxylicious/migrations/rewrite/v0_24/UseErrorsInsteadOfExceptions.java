/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.migrations.rewrite.v0_24;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;

/**
 * Removes calls to `exception()` from Apaches Error enum. That is it transforms {@code Errors.GROUP_AUTHORIZATION_FAILED.exception()} into {@code Errors.GROUP_AUTHORIZATION_FAILED}
 */
public class UseErrorsInsteadOfExceptions extends Recipe {

    /**
     * Instantiates an instance
     */
    public UseErrorsInsteadOfExceptions() {
        // Intentionally empty
    }

    @Override
    public String getDisplayName() {
        return "Unwrap `.exception()` calls in `errorResponse`";
    }

    @Override
    public String getDescription() {
        return "Removes redundant `.exception()` invocations passed as the third argument to `errorResponse(...)`.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new ErrorsConverterJavaIsoVisitor();
    }

    private static class ErrorsConverterJavaIsoVisitor extends JavaIsoVisitor<ExecutionContext> {

        @Override
        public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
            J.MethodInvocation mi = super.visitMethodInvocation(method, ctx);

            // Target errorResponse invocations with 3 arguments
            List<Expression> arguments = mi.getArguments();
            if ("errorResponse".equals(mi.getSimpleName()) && arguments.size() == 3) {
                Expression thirdArg = arguments.get(2);
                if (thirdArg instanceof J.MethodInvocation invocation) {
                    Expression innerExpression = invocation.getSelect();
                    List<Expression> newArgs = new ArrayList<>(arguments);
                    newArgs.set(2, Objects.requireNonNull(innerExpression).withPrefix(thirdArg.getPrefix()));

                    maybeRemoveImport("org.apache.kafka.common.protocol.Errors");
                    maybeAddImport("io.kroxylicious.kafka.common.protocol.Errors");
                    return mi.withArguments(newArgs);
                }
            }
            return mi;
        }
    }
}
