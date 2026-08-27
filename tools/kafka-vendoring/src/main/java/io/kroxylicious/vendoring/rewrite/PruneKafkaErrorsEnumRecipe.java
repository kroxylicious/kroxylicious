/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.vendoring.rewrite;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.NullMarked;
import org.openrewrite.ExecutionContext;
import org.openrewrite.FindSourceFiles;
import org.openrewrite.Preconditions;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.JavadocVisitor;
import org.openrewrite.java.RemoveUnusedImports;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Javadoc;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.Statement;
import org.openrewrite.marker.Markers;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Strips client exception builders, fields, methods, and imports from Kafka Errors enum.to Apache Kafka's hand rolled Errors enum to make it part of the Kroxylicious API
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

        private static final String BUILDER_METHOD_NAME = "builder";
        private static final String EXCEPTION = "exception";
        private static final Set<String> REMOVED_FIELDS = Set.of(BUILDER_METHOD_NAME, EXCEPTION, "CLASS_TO_ERROR");
        private static final Set<String> REMOVED_METHODS = Set.of(EXCEPTION, "forException", "maybeThrow", "exceptionName", "main", "toHtml", "maybeUnwrapException");

        private final JavaTemplate messageFieldTemplate = JavaTemplate.builder("private final String message;").contextSensitive().build();

        private final JavaTemplate messageMethodBody = JavaTemplate.builder("""
                if (this.message != null) {
                    return this.message;
                }
                return this.toString();
                """).contextSensitive().build();

        private final JavaTemplate constructorTemplate = JavaTemplate.builder(
                        """
                                Errors(int code, String message) {
                                    this.code = (short) code;
                                    this.message = message;
                                }""")
                .contextSensitive()
                .build();

        @Override
        @NonNull
        protected JavadocVisitor<ExecutionContext> getJavadocVisitor() {
            return new PruneJavadocVisitor();
        }

        @Override
        @NullMarked
        public J.ClassDeclaration visitClassDeclaration(J.ClassDeclaration classDeclaration, ExecutionContext ctx) {
            J.ClassDeclaration cd = classDeclaration;
            boolean hasMessageField = findFieldByName(cd, "message") != null;

            if (!hasMessageField) {
                Statement codeField = findFieldByName(cd, "code");

                if (codeField != null) {
                    cd = messageFieldTemplate.apply(updateCursor(cd), codeField.getCoordinates().after());
                }
            }

            return (J.ClassDeclaration) super.visitClassDeclaration(cd, ctx);
        }

        @Override
        @NonNull
        public J.EnumValue visitEnumValue(@NonNull J.EnumValue enumConstant, @NonNull ExecutionContext ctx) {
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
        public @Nullable J.VariableDeclarations visitVariableDeclarations(J.VariableDeclarations multiVariable, @NonNull ExecutionContext ctx) {
            if (multiVariable.getVariables().stream().anyMatch(v -> REMOVED_FIELDS.contains(v.getSimpleName()))) {
                return null; // Remove 'builder' field
            }
            return (J.VariableDeclarations) super.visitVariableDeclarations(multiVariable, ctx);
        }

        @NonNull
        private J.MethodDeclaration visitConstructorDeclaration(J.MethodDeclaration md) {
            return constructorTemplate.apply(updateCursor(md), md.getCoordinates().replace());
        }

        @Override
        public @Nullable J.MethodDeclaration visitMethodDeclaration(@NonNull J.MethodDeclaration methodDeclaration, @NonNull ExecutionContext ctx) {

            // Remove exception factory methods
            if (REMOVED_METHODS.contains(methodDeclaration.getSimpleName())) {
                return null;
            }

            if (methodDeclaration.isConstructor() && hasBuilderParameter(methodDeclaration)) {
                return visitConstructorDeclaration(methodDeclaration);
            }

            var md = (J.MethodDeclaration) super.visitMethodDeclaration(methodDeclaration, ctx);

            J.Block body = md.getBody();
            if ("message".equals(md.getSimpleName()) && body != null) {
                String currentBody = body.printTrimmed(getCursor());
                if (currentBody.contains(EXCEPTION)) {
                    return messageMethodBody.apply(updateCursor(md), md.getCoordinates().replaceBody());
                }
            }
            return md;
        }

        @Override
        @NonNull
        public J.CompilationUnit visitCompilationUnit(@NonNull J.CompilationUnit cu, @NonNull ExecutionContext ctx) {
            J.CompilationUnit c = (J.CompilationUnit) super.visitCompilationUnit(cu, ctx);
            doAfterVisit(new RemoveUnusedImports().getVisitor());
            return c;
        }

        @Override
        public @Nullable J visitIf(@NonNull J.If ifSeq, @NonNull ExecutionContext ctx) {
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
                body = block.getStatements().getFirst();
            }
            if (body instanceof J.MethodInvocation mi) {
                Expression selectExpression = mi.getSelect();
                return selectExpression != null && selectExpression.toString().endsWith("CLASS_TO_ERROR") && "put".equals(mi.getSimpleName());
            }
            return false;
        }

        @Nullable
        private static Statement findFieldByName(J.ClassDeclaration cd, String fieldName) {
            return cd.getBody().getStatements().stream()
                    .filter(J.VariableDeclarations.class::isInstance)
                    .map(J.VariableDeclarations.class::cast)
                    .filter(vd -> vd.getVariables().stream()
                            .anyMatch(v -> v.getSimpleName().equals(fieldName)))
                    .findFirst()
                    .orElse(null);
        }

        private boolean hasBuilderParameter(J.MethodDeclaration md) {
            return md.getParameters().stream()
                    .filter(J.VariableDeclarations.class::isInstance)
                    .map(J.VariableDeclarations.class::cast)
                    .flatMap(vd -> vd.getVariables().stream())
                    .anyMatch(v -> BUILDER_METHOD_NAME.equals(v.getSimpleName()));
        }

        @NonNull
        private J.EnumValue removeBuilderArg(JavaType.Method constructorType, J.NewClass initializer, J.EnumValue enumValue) {
            List<String> paramNames = constructorType.getParameterNames();
            List<Expression> args = initializer.getArguments();
            List<Expression> safeArgs = new ArrayList<>();
            if (paramNames.contains(BUILDER_METHOD_NAME)) {
                for (int i = 0; i < args.size(); i++) {
                    Expression arg = args.get(i);
                    String paramName = (i < paramNames.size()) ? paramNames.get(i) : "";

                    if (BUILDER_METHOD_NAME.equals(paramName)) {
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
            public @Nullable Javadoc visitSee(@NonNull Javadoc.See see, @NonNull ExecutionContext ctx) {
                // Check if the @see reference targets SslTransportLayer
                if (see.printTrimmed(getCursor()).contains("SslTransportLayer")) {
                    return null; // Safely deletes the @see tag node from the Javadoc AST
                }
                return super.visitSee(see, ctx);
            }

            @Override
            @NonNull
            public Javadoc visitDocComment(@NonNull Javadoc.DocComment docComment, @NonNull ExecutionContext ctx) {
                Javadoc.DocComment originalComment = (Javadoc.DocComment) super.visitDocComment(docComment, ctx);
                List<Javadoc> body = originalComment.getBody();
                J host = getCursor().firstEnclosing(J.class);

                if (host instanceof J.MethodDeclaration method && method.getSimpleName().equals("forCode")) {
                    if (docComment.print(getCursor()).contains(EXCEPTION)) {
                        List<Javadoc> newBody = buildReplacementForCodeComment(docComment, method, body);
                        return originalComment.withBody(newBody);
                    }
                }
                else if (host instanceof J.ClassDeclaration) {
                    List<Javadoc> cleanedBody = pruneLineBreaks(body);

                    if (cleanedBody.size() == body.size()) {
                        return originalComment;
                    }
                    return originalComment.withBody(cleanedBody);

                }
                return docComment;
            }

            @NonNull
            private List<Javadoc> buildReplacementForCodeComment(@NonNull Javadoc.DocComment docComment, J.MethodDeclaration method, List<Javadoc> body) {
                String margin = extractMargin(body);
                J.Identifier codeParamName = findParameterNamed(method, "code");
                Markers markers = existingMarkers(docComment);
                Javadoc.Text returnText = new Javadoc.Text(Tree.randomId(), Markers.EMPTY,
                        " the Errors enum entry for the code. Returns {@code Errors.UNKNOWN_SERVER_ERROR} for all unmapped codes.");
                return List.of(
                        new Javadoc.LineBreak(Tree.randomId(), margin, markers),
                        new Javadoc.Text(Tree.randomId(), Markers.EMPTY, " Map the code for an Error to its Entry in the enum"),
                        new Javadoc.LineBreak(Tree.randomId(), margin + " ", Markers.EMPTY),
                        new Javadoc.Parameter(Tree.randomId(), Markers.EMPTY, List.of(), codeParamName.withPrefix(Space.format(" ")), null,
                                List.of(new Javadoc.Text(Tree.randomId(), Markers.EMPTY, " the {@code short} to map"))),
                        new Javadoc.LineBreak(Tree.randomId(), margin + " ", Markers.EMPTY),
                        new Javadoc.Return(Tree.randomId(), Markers.EMPTY, List.of(returnText)),
                        new Javadoc.LineBreak(Tree.randomId(), margin, Markers.EMPTY),
                        new Javadoc.LineBreak(Tree.randomId(), margin.substring(0, margin.lastIndexOf('*')), Markers.EMPTY));
            }

            @NonNull
            private static Markers existingMarkers(@NonNull Javadoc.DocComment docComment) {
                return docComment.getBody().isEmpty() ? Markers.EMPTY : docComment.getBody().getFirst().getMarkers();
            }

            @NonNull
            private static String extractMargin(List<Javadoc> body) {
                return body.stream()
                        .filter(Javadoc.LineBreak.class::isInstance)
                        .map(Javadoc.LineBreak.class::cast)
                        .map(Javadoc.LineBreak::getMargin)
                        .findFirst()
                        .orElse("\n * ");
            }

            @NonNull
            private static List<Javadoc> pruneLineBreaks(List<Javadoc> body) {
                List<Javadoc> cleanedBody = new ArrayList<>(body);
                if (body.isEmpty()) {
                    return body;
                }
                if (cleanedBody.getLast() instanceof Javadoc.Text text && text.getText().trim().isEmpty()) {
                    cleanedBody.removeLast();
                }
                if (cleanedBody.getLast() instanceof Javadoc.LineBreak lastLine) {
                    String margin = lastLine.getMargin();
                    if (margin.endsWith("*")) {
                        cleanedBody.removeLast();
                        cleanedBody.addLast(lastLine.withMargin(margin.substring(0, margin.lastIndexOf('*'))));
                    }
                }
                return cleanedBody;
            }

        }

        @NonNull
        private static J.Identifier findParameterNamed(J.MethodDeclaration method, String paramName) {
            return method.getParameters().stream()
                    .filter(J.VariableDeclarations.class::isInstance)
                    .map(J.VariableDeclarations.class::cast)
                    .flatMap(vd -> vd.getVariables().stream())
                    .filter(v -> paramName.equals(v.getSimpleName()))
                    .map(J.VariableDeclarations.NamedVariable::getName)
                    .findFirst()
                    .orElseThrow();
        }
    }

}
