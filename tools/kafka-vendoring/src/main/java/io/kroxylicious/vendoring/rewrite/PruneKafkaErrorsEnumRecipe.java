/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.vendoring.rewrite;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.openrewrite.ExecutionContext;
import org.openrewrite.FindSourceFiles;
import org.openrewrite.Preconditions;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.JavadocVisitor;
import org.openrewrite.java.RemoveUnusedImports;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Javadoc;
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
        return Preconditions.check(new FindSourceFiles("**/Errors.java"), new ErrorsEnumVisitor());
    }

    private static class ErrorsEnumVisitor extends JavaVisitor<ExecutionContext> {

        private final Set<String> REMOVED_FIELDS = Set.of("builder", "exception", "CLASS_TO_ERROR");
        private final Set<String> REMOVED_METHODS = Set.of("exception", "forException", "maybeThrow", "exceptionName", "main", "toHtml");

        private final JavaTemplate messageFieldTemplate = JavaTemplate.builder("private final String message;").contextSensitive().build();

        private final JavaTemplate messageMethodBody = JavaTemplate.builder("return this.message;").contextSensitive().build();

        private final JavaTemplate constructorTemplate = JavaTemplate.builder(
                        "Errors(int code, String defaultMessage) {\n" + "    this.code = (short) code;\n" + "    this.message = defaultMessage;\n" + "}")
                .contextSensitive()
                .build();

        @Override
        protected JavadocVisitor<ExecutionContext> getJavadocVisitor() {
            return new PruneJavadocVisitor();
        }

        @Override
        public J.ClassDeclaration visitClassDeclaration(J.ClassDeclaration classDeclaration, ExecutionContext ctx) {
            J.ClassDeclaration cd = classDeclaration;
            boolean hasMessageField = findFieldByName(classDeclaration, "message") != null;

            if (!hasMessageField) {
                Statement codeField = findFieldByName(cd, "code");

                if (codeField != null) {
                    cd = messageFieldTemplate.apply(updateCursor(cd), codeField.getCoordinates().after());
                }
            }
            return (J.ClassDeclaration) super.visitClassDeclaration(cd, ctx);
        }

        @Override
        public J.EnumValue visitEnumValue(J.EnumValue enumConstant, ExecutionContext ctx) {
            J.EnumValue ec = (J.EnumValue) super.visitEnumValue(enumConstant, ctx);
            J.NewClass initializer = ec.getInitializer();
            if (initializer == null) {
                return ec;
            }

            JavaType.Method constructorType = initializer.getConstructorType();
            if (constructorType == null) {
                return ec;
            }

            return removeBuilderArg(constructorType, initializer, ec);
        }

        @Override
        public J.VariableDeclarations visitVariableDeclarations(J.VariableDeclarations multiVariable, ExecutionContext ctx) {
            if (multiVariable.getVariables().stream().anyMatch(v -> REMOVED_FIELDS.contains(v.getSimpleName()))) {
                return null; // Remove 'builder' field
            }
            findVariableByName("message", multiVariable);

            return (J.VariableDeclarations) super.visitVariableDeclarations(multiVariable, ctx);
        }

        @NonNull
        private J.MethodDeclaration visitConstructorDeclaration(J.MethodDeclaration md) {
            return constructorTemplate.apply(updateCursor(md), md.getCoordinates().replace());
        }

        @Override
        public J.MethodDeclaration visitMethodDeclaration(J.MethodDeclaration methodDeclaration, ExecutionContext ctx) {

            // Remove exception factory methods
            if (REMOVED_METHODS.contains(methodDeclaration.getSimpleName())) {
                return null;
            }

            if (methodDeclaration.isConstructor() && findParameterByName(methodDeclaration, "builder") != null) {
                return visitConstructorDeclaration(methodDeclaration);
            }

            var md = (J.MethodDeclaration) super.visitMethodDeclaration(methodDeclaration, ctx);

            if ("message".equals(md.getSimpleName()) && md.getBody() != null) {
                String currentBody = md.getBody().printTrimmed(getCursor());
                if (!currentBody.contains("this.message")) {
                    return messageMethodBody.apply(updateCursor(md), md.getCoordinates().replaceBody());
                }
            }
            return md;
        }

        @Override
        public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
            J.CompilationUnit c = (J.CompilationUnit) super.visitCompilationUnit(cu, ctx);
            doAfterVisit(new RemoveUnusedImports().getVisitor());
            return c;
        }

        @Override
        @Nullable
        public J visitIf(J.If ifSeq, ExecutionContext ctx) {
            J.If i = (J.If) super.visitIf(ifSeq, ctx);

            if (isClassToErrorIf(i)) {
                return null; // Deletes the if-statement from the parent block
            }
            return i;
        }

        private boolean isClassToErrorIf(J.If ifStmt) {
            Statement body = ifStmt.getThenPart();
            if (body instanceof J.Block block) {
                if (block.getStatements().isEmpty()) {
                    return false;
                }
                body = block.getStatements().get(0);
            }
            if (body instanceof J.MethodInvocation mi) {
                Expression selectExpression = mi.getSelect();
                return selectExpression != null && selectExpression.toString().endsWith("CLASS_TO_ERROR") && "put".equals(mi.getSimpleName());
            }
            return false;
        }

        @Nullable
        private static Statement findFieldByName(J.ClassDeclaration cd, String fieldName) {
            return cd.getBody().getStatements().stream().filter(s -> s instanceof J.VariableDeclarations vd && findVariableByName(fieldName, vd) != null).findFirst()
                    .orElse(null);
        }

        private static J.VariableDeclarations.NamedVariable findVariableByName(String fieldName, J.VariableDeclarations vd) {
            return vd.getVariables().stream().filter(v -> fieldName.equals(v.getSimpleName())).findFirst().orElse(null);
        }

        @Nullable
        private J.VariableDeclarations.NamedVariable findParameterByName(J.MethodDeclaration md, String parameterName) {
            return md.getParameters().stream().filter(J.VariableDeclarations.class::isInstance).map(J.VariableDeclarations.class::cast)
                    .flatMap(vd -> vd.getVariables().stream()).filter(v -> parameterName.equals(v.getSimpleName())).findFirst().orElse(null);
        }

        @NonNull
        private J.EnumValue removeBuilderArg(JavaType.Method constructorType, J.NewClass initializer, J.EnumValue enumValue) {
            List<String> paramNames = constructorType.getParameterNames();
            List<Expression> args = initializer.getArguments();
            List<Expression> safeArgs = new ArrayList<>();
            if (paramNames.contains("builder")) {
                for (int i = 0; i < args.size(); i++) {
                    Expression arg = args.get(i);
                    String paramName = (i < paramNames.size()) ? paramNames.get(i) : "";

                    if ("builder".equals(paramName)) {
                        // Prune import for the referenced exception type in UnknownServerException::new
                        if (arg instanceof J.MemberReference mr && mr.getContaining().getType() instanceof JavaType.FullyQualified fq) {
                            maybeRemoveImport(fq.getFullyQualifiedName());
                        }
                        maybeRemoveImport("java.util.function.Function");
                    }
                    else {
                        safeArgs.add(arg);
                    }
                }

                if (safeArgs.size() != args.size()) {
                    return enumValue.withInitializer(initializer.withArguments(safeArgs));
                }
            }
            return enumValue;
        }

        private class PruneJavadocVisitor extends JavadocVisitor<ExecutionContext> {
            PruneJavadocVisitor() {
                super(ErrorsEnumVisitor.this);
            }

            @Override
            public Javadoc visitSee(Javadoc.See see, ExecutionContext ctx) {
                // Check if the @see reference targets SslTransportLayer
                if (see.printTrimmed(getCursor()).contains("SslTransportLayer")) {
                    return null; // Safely deletes the @see tag node from the Javadoc AST
                }
                return super.visitSee(see, ctx);
            }

            @Override
            public Javadoc visitDocComment(Javadoc.DocComment docComment, ExecutionContext ctx) {
                Javadoc.DocComment originalComment = (Javadoc.DocComment) super.visitDocComment(docComment, ctx);
                List<Javadoc> body = originalComment.getBody();
                List<Javadoc> cleanedBody = pruneLineBreaks(body);

                if (cleanedBody.size() == body.size()) {
                    return originalComment;
                }
                return originalComment.withBody(cleanedBody);
            }

            @NonNull
            private static List<Javadoc> pruneLineBreaks(List<Javadoc> body) {
                List<Javadoc> cleanedBody = new ArrayList<>(body);

                if (cleanedBody.getLast() instanceof Javadoc.Text text && text.getText().trim().isEmpty()) {
                    cleanedBody.removeLast();
                }
                if (cleanedBody.getLast() instanceof Javadoc.LineBreak lastLine) {
                    String margin = lastLine.getMargin();
                    if (margin.contains("*")) {
                        String newMargin = margin.substring(0, margin.indexOf('*'));
                        cleanedBody.removeLast();
                        cleanedBody.addLast(lastLine.withMargin(newMargin));
                    }
                }
                return cleanedBody;
            }

        }
    }
}
