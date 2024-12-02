/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.AnnotationExpr;
import com.github.javaparser.ast.expr.ArrayInitializerExpr;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.BooleanLiteralExpr;
import com.github.javaparser.ast.expr.ClassExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.InstanceOfExpr;
import com.github.javaparser.ast.expr.MarkerAnnotationExpr;
import com.github.javaparser.ast.expr.MemberValuePair;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.Name;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.NormalAnnotationExpr;
import com.github.javaparser.ast.expr.SimpleName;
import com.github.javaparser.ast.expr.SingleMemberAnnotationExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.expr.ThisExpr;
import com.github.javaparser.ast.expr.TypeExpr;
import com.github.javaparser.ast.expr.TypePatternExpr;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.stmt.IfStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.github.javaparser.ast.stmt.Statement;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.Type;
import com.github.javaparser.ast.type.VoidType;

import io.kroxylicious.tools.schema.model.SchemaObject;
import io.kroxylicious.tools.schema.model.SchemaObjectBuilder;
import io.kroxylicious.tools.schema.model.SchemaType;
import io.kroxylicious.tools.schema.model.XKubeListType;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Java code generation from a SchemaObject.
 */
public class CodeGen {

    private final IdVisitor idVisitor;
    private final Diagnostics diagnostics;
    private final Map<String, String> existingClasses;
    private final String nullableAnnotation;
    private final String nonNullAnnotation;

    public CodeGen(Diagnostics diagnostics,
                   IdVisitor idVisitor,
                   Map<String, String> existingClasses,
                   String nullableAnnotation,
                   String nonNullAnnotation) {
        this.diagnostics = Objects.requireNonNull(diagnostics);
        this.idVisitor = Objects.requireNonNull(idVisitor);
        this.existingClasses = existingClasses;
        this.nullableAnnotation = nullableAnnotation;
        this.nonNullAnnotation = nonNullAnnotation;
    }

    SchemaObject resolveRef(SchemaObject root, SchemaObject schema) {
        var ref = schema.getRef() == null ? null : URI.create(schema.getRef());
        if (ref != null) {
            if (ref.isAbsolute()) {
                diagnostics.reportFatal("Use of an absolute URI in $ref is not supported");
                return new SchemaObject();
            }
            else if ((ref.getPath() == null
                        || ref.getPath().isEmpty())
                    && ref.getFragment() != null) {
                return resolveInternalFragmentRef(root, ref);
            }
            // Two possibilities: a local ref point to a file which we should be compiling
            // or a local ref pointing to a file which we should _not be compiling_
            // In the _not compiling_ case the referring file depends on decls which we won't emit
            // If the java for the referred to file is out of date then we'll likely generate the wrong thing

            URI sought = URI.create(root.getId()).resolve(ref);
            var resolved = idVisitor.resolve(sought);
            if (resolved != null) {
                return resolved;
            }
            else {
                diagnostics.reportError("Canot resolve $ref (but $ref not fully supported) {}", ref);
                return new SchemaObject();
            }
        }
        else {
            return schema;
        }
    }

    @NonNull
    private SchemaObject resolveInternalFragmentRef(SchemaObject root, URI ref) {
        if (ref.getFragment().startsWith("/definitions/")) {
            Map<String, SchemaObject> defs = root.getDefinitions();
            if (defs != null) {
                String name = ref.getFragment().substring("/definitions/".length());
                SchemaObject object = defs.get(name);
                if (object != null) {
                    return new SchemaObjectBuilder(object).withJavaType(name).build();
                }
            }
            diagnostics.reportFatal("Couldn't resolve $ref " + ref);
            return new SchemaObject();
        }
        diagnostics.reportFatal("$ref not fully supported");
        return new SchemaObject();
    }

    @SuppressWarnings("java:S1192")
    Type genTypeName(String pkg, SchemaObject root, SchemaObject schema) {
        Objects.requireNonNull(schema);
        List<SchemaType> type = schema.getType();
        if (type == null || type.isEmpty()) {
            // unconstrained => union of all types
            type = new ArrayList<>(SchemaType.all());
        }
        else {
            type = new ArrayList<>(type);
        }
        if (type.size() == 1) {
            return switch (type.get(0)) {
                case NULL -> new ClassOrInterfaceType(null, "java.lang.Object");
                case BOOLEAN -> new ClassOrInterfaceType(null, "java.lang.Boolean");
                case INTEGER -> new ClassOrInterfaceType(null, "java.lang.Long");
                case NUMBER -> new ClassOrInterfaceType(null, "java.lang.Double");
                case STRING -> {
                    if (schema.getFormat() != null) {
                        yield new ClassOrInterfaceType(null, switch (schema.getFormat()) {
                            case "uri" -> "java.net.URI";
                            default -> "java.lang.Object";
                        });
                    }
                    else {
                        yield new ClassOrInterfaceType(null, "java.lang.String");
                    }
                }
                case ARRAY -> genCollectionOrMapType(pkg, root, schema);
                case OBJECT -> {
                    // TODO or Map or ObjectNode if x-kubernetes-preserve-unknown-keys
                    String fqName = pkg + "." + className(schema);
                    String orDefault = existingClasses.getOrDefault(fqName, fqName);
                    yield new ClassOrInterfaceType(null, orDefault);
                }
            };
        }
        else {
            return new ClassOrInterfaceType(null, "java.lang.Object");
        }
    }

    @NonNull
    private ClassOrInterfaceType genCollectionOrMapType(String pkg, SchemaObject root, SchemaObject schema) {
        SchemaObject itemSchema = resolveRef(root, schema.getItems().get(0));
        var itemType = genTypeName(pkg, root, itemSchema);
        XKubeListType xKubeListType = schema.getXKubernetesListType();
        if (xKubeListType == null
                || xKubeListType == XKubeListType.ATOMIC) {
            return new ClassOrInterfaceType(null, new SimpleName("java.util.List"),
                    new NodeList<>(itemType));
        }
        else if (xKubeListType == XKubeListType.SET) {
            return new ClassOrInterfaceType(null, new SimpleName("java.util.Set"),
                    new NodeList<>(itemType));
        }
        else if (xKubeListType == XKubeListType.MAP) {
            List<String> keyPropertyNames = schema.getXKubernetesListMapKeys();
            Type keyType;
            if (keyPropertyNames == null
                    || keyPropertyNames.isEmpty()) {
                diagnostics.reportError("'x-kubernetes-list-map-keys' property is required when 'x-kubernetes-list-type: map'");
                // Use some type so we can keep going, even though the Java won't compile
                keyType = genErrorType();
            }
            else if (keyPropertyNames.size() > 1) {
                // x-kubernetes-list-map-keys=['foo', 'bar'] should result in an inner class to represent the compound key
                diagnostics.reportError("'x-kubernetes-list-map-keys' property with multiple values is not yet supported");
                // Use some type so we can keep going, even though the Java won't compile
                keyType = genErrorType();
            }
            else {
                SchemaObject keySchema = itemSchema.getProperties().get(keyPropertyNames.get(0));
                keyType = genTypeName(pkg, root, keySchema);
            }
            return new ClassOrInterfaceType(null, new SimpleName("java.util.Map"),
                    new NodeList<>(keyType, itemType));
        }
        else {
            diagnostics.reportError("Unsupported 'x-kubernetes-list-type': " + xKubeListType);
            return genErrorType();
        }
    }

    /**
     * Sometimes it's better to generate a type, even in the presence of an invalid schema,
     * so we can at least generate some java code and report more errors to the user.
     * @return
     */
    @NonNull
    private static ClassOrInterfaceType genErrorType() {
        return new ClassOrInterfaceType(null, "code.generation.Error");
    }

    List<CompilationUnit> genDecls(SchemaInput input) {
        var result = new ArrayList<CompilationUnit>();
        // TODO visit the subschemas given them javaType names if they don't have them already.
        // If loaded from URI ending /x or /x.yaml or /X or /X.yaml
        // root schema = X
        // definions = the name
        // subschema of root via property foo = XFoo
        // subschema of root via item of array foos = XFoo
        SchemaObject root = input.rootSchema();
        root.visitSchemas(input.schemaPath().toUri(), new CodeGenVisitor(input, result));
        return result;
    }

    /**
     * Generate a type declaration for the given schema, or null if the type is declared externally
     *
     * @param pkg
     * @param schema
     */
    private @Nullable CompilationUnit genDecl(String pkg, SchemaObject schema, String path, URI base) {
        List<SchemaType> type = schema.getType();
        if (type == null) {
            type = SchemaType.all();
        }
        if (type.size() == 1) {
            return switch (type.get(0)) {
                case OBJECT -> genClass(pkg, idVisitor.resolve(base), schema, path);
                case ARRAY, STRING, INTEGER, NUMBER, BOOLEAN, NULL -> null;
            };
        }
        else {
            throw new UnsupportedOperationException("Can't handle union types yet");
        }
    }

    public static boolean isTypeGenerated(SchemaObject schemaObject) {
        return List.of(SchemaType.OBJECT).equals(schemaObject.getType());
    }

    Map<String, String> seen = new HashMap<>();

    @Nullable
    private CompilationUnit genClass(String pkg, SchemaObject root, SchemaObject schema, String path) {
        assert (isTypeGenerated(schema));
        String name = className(schema);
        if (existingClasses.containsKey(pkg + "." + name)) {
            return null;
        }
        String oldPath = seen.put(name, path);
        if (oldPath != null) {
            diagnostics.reportFatal(
                    "Already generated {} when visited {}, now trying to generate it again when visiting {}",
                    name,
                    oldPath,
                    path);
        }
        Map<String, SchemaObject> properties = schema.getProperties() == null ? Map.of() : schema.getProperties();

        Set<String> required = schema.getRequired() == null ? Set.of() : schema.getRequired();
        CompilationUnit cu = new CompilationUnit();
        cu.setPackageDeclaration(new PackageDeclaration(new Name(pkg)));

        ClassOrInterfaceDeclaration clz = cu.addClass(name,
                Modifier.Keyword.PUBLIC);
        String classDescription = schema.getDescription();
        if (classDescription == null) {
            classDescription = "Auto-generated class representing the schema at " + path + ".";
        }
        clz.setJavadocComment(classDescription);

        // @javax.annotation.processing.Generated("...")
        clz.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("javax.annotation.processing.Generated"),
                new StringLiteralExpr(getClass().getName())));
        // @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        clz.addAnnotation(new SingleMemberAnnotationExpr(
                new Name("com.fasterxml.jackson.annotation.JsonInclude"),
                new FieldAccessExpr(new TypeExpr(new ClassOrInterfaceType(null, "com.fasterxml.jackson.annotation.JsonInclude.Include")), "NON_NULL")));

        // @com.fasterxml.jackson.annotation.JsonPropertyOrder({...properties...})
        if (!properties.isEmpty()) {
            clz.addAnnotation(new SingleMemberAnnotationExpr(
                    new Name("com.fasterxml.jackson.annotation.JsonPropertyOrder"),
                    new ArrayInitializerExpr(new NodeList<>(properties.keySet().stream().map(x -> (Expression) new StringLiteralExpr(x)).toList()))));
        }
        // @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
        clz.addAnnotation(new NormalAnnotationExpr(new Name("com.fasterxml.jackson.databind.annotation.JsonDeserialize"),
                new NodeList<>(new MemberValuePair("using", new ClassExpr(new ClassOrInterfaceType(null, "com.fasterxml.jackson.databind.JsonDeserializer.None"))))));

        for (var entry : properties.entrySet()) {
            String propName = entry.getKey();
            var propSchema = resolveRef(root, entry.getValue());
            var propType = genTypeName(pkg, root, propSchema);
            clz.addMember(mkPropertyField(propName, required.contains(propName), propType));
        }

        mkConstructors(pkg, root, properties, required, clz);

        for (var entry : properties.entrySet()) {
            String propName = entry.getKey();
            var propSchema = resolveRef(root, entry.getValue());
            var propType = genTypeName(pkg, root, propSchema);
            clz.addMember(mkPropertyGetterMethod(propSchema.getDescription(), propName, propType, required.contains(propName)));
            clz.addMember(mkPropertySetterMethod(propSchema.getDescription(), propName, propType, required.contains(propName)));
        }

        addToStringMethod(clz, properties);
        addHashCodeMethod(clz, properties);
        addEqualsMethod(pkg, clz, properties);
        return cu;
    }

    private void mkConstructors(String pkg, SchemaObject root, Map<String, SchemaObject> properties, Set<String> required, ClassOrInterfaceDeclaration clz) {

        // Add the all properties ctor
        ConstructorDeclaration decl = mkConstructor(pkg, root, clz,
                "All properties constructor.", properties, required, (propName, schemaObject) -> List.of(
                        mkAtJsonProperty(propName, required.contains(propName))));
        decl.addAnnotation(new MarkerAnnotationExpr("com.fasterxml.jackson.annotation.JsonCreator"));
        clz.addMember(decl);

    }

    @NonNull
    private ConstructorDeclaration mkConstructor(String pkg,
                                                 SchemaObject root,
                                                 ClassOrInterfaceDeclaration clz,
                                                 String javadoc,
                                                 Map<String, SchemaObject> properties,
                                                 Set<String> required,
                                                 BiFunction<String, SchemaObject, List<AnnotationExpr>> annotator) {
        ConstructorDeclaration ctor = new ConstructorDeclaration();

        ctor.setJavadocComment(properties.keySet().stream()
                .map(propName -> {
                    String s = "@param " + fieldName(propName) + " The value of the {@code " + propName + "} property.";
                    if (required.contains(propName)) {
                        s += " This is a required property.";
                    }
                    else {
                        s += " This is an optional property.";
                    }
                    return s;
                })
                .collect(Collectors.joining("\n", javadoc + "\n", "")));
        ctor.setModifiers(Modifier.Keyword.PUBLIC);
        ctor.setName(clz.getName());

        var pl = properties.entrySet().stream()
                .map(entry -> {
                    var propSchema = resolveRef(root, entry.getValue());
                    var propType = genTypeName(pkg, root, propSchema);
                    NodeList<AnnotationExpr> annotations = NodeList.nodeList(annotator.apply(entry.getKey(), entry.getValue()));
                    annotations.add(mkNullableAnnotation(required.contains(entry.getKey())));
                    return new Parameter(propType, fieldName(entry.getKey()))
                            .setAnnotations(annotations);
                }).toList();
        ctor.setParameters(NodeList.nodeList(pl));

        var assignments = properties.keySet().stream()
                .map(propName -> {
                    String fieldName = fieldName(propName);
                    return (Statement) new ExpressionStmt(new AssignExpr(new FieldAccessExpr(
                            new ThisExpr(), fieldName),
                            required.contains(propName) ? new MethodCallExpr("java.util.Objects.requireNonNull", new NameExpr(fieldName)) : new NameExpr(fieldName),
                            AssignExpr.Operator.ASSIGN));
                })
                .toList();
        ctor.setBody(new BlockStmt(NodeList.nodeList(assignments)));
        return ctor;
    }

    @NonNull
    private static String className(SchemaObject schema) {
        if (schema.getJavaType() != null) {
            return schema.getJavaType();
        }
        else {
            throw new IllegalStateException("Schema lacks explicit or generated $javaType");
        }
    }

    private static void addToStringMethod(ClassOrInterfaceDeclaration clz, Map<String, SchemaObject> properties) {
        Expression expr = new StringLiteralExpr(clz.getNameAsString() + "[");
        boolean first = true;

        for (var entry : properties.entrySet()) {
            String propName = entry.getKey();
            StringLiteralExpr x;
            if (first) {
                x = new StringLiteralExpr(propName + ": ");
            }
            else {
                x = new StringLiteralExpr(", " + propName + ": ");
            }
            first = false;
            expr = new BinaryExpr(expr, x, BinaryExpr.Operator.PLUS);
            expr = new BinaryExpr(
                    expr,
                    new FieldAccessExpr(new ThisExpr(), fieldName(propName)),
                    BinaryExpr.Operator.PLUS);
        }
        expr = new BinaryExpr(expr, new StringLiteralExpr("]"), BinaryExpr.Operator.PLUS);

        clz.addMethod("toString", Modifier.Keyword.PUBLIC)
                .setType("java.lang.String")
                .addAnnotation(mkAtOverride())
                .setBody(new BlockStmt(new NodeList<>(new ReturnStmt(expr))));
    }

    private static void addHashCodeMethod(ClassOrInterfaceDeclaration clz, Map<String, SchemaObject> properties) {
        NodeList<Expression> args = new NodeList<>();
        for (var entry : properties.entrySet()) {
            String propName = entry.getKey();
            args.add(new FieldAccessExpr(new ThisExpr(), fieldName(propName)));
        }

        clz.addMethod("hashCode", Modifier.Keyword.PUBLIC)
                .setType("int")
                .addAnnotation(mkAtOverride())
                .setBody(new BlockStmt(new NodeList<>(new ReturnStmt(new MethodCallExpr("java.util.Objects.hash")
                        .setArguments(args)))));
    }

    private static void addEqualsMethod(
                                        String pkg,
                                        ClassOrInterfaceDeclaration clz,
                                        Map<String, SchemaObject> properties) {
        String className = clz.getNameAsString();
        Expression expr;
        String otherParamName = "other";
        String narrowedOtherName = otherParamName + className;
        if (properties.isEmpty()) {
            expr = new BooleanLiteralExpr(true);
        }
        else {
            Expression operand = null;
            for (var entry : properties.entrySet()) {
                String propName = entry.getKey();
                MethodCallExpr call = new MethodCallExpr("java.util.Objects.equals")
                        .setArguments(new NodeList<>(
                                new FieldAccessExpr(new ThisExpr(), fieldName(propName)),
                                new FieldAccessExpr(new NameExpr(narrowedOtherName), fieldName(propName))));
                if (operand == null) {
                    operand = call;
                }
                else {
                    operand = new BinaryExpr(
                            operand,
                            call,
                            BinaryExpr.Operator.AND);
                }
            }
            expr = operand;
        }

        var stmt = new IfStmt(new BinaryExpr(
                new ThisExpr(),
                new NameExpr(otherParamName),
                BinaryExpr.Operator.EQUALS),
                new ReturnStmt(new BooleanLiteralExpr(true)),
                new IfStmt(new InstanceOfExpr(
                        new NameExpr(otherParamName),
                        new ClassOrInterfaceType(null, className),
                        new TypePatternExpr(new NodeList<>(), new ClassOrInterfaceType(null, pkg + "." + className), new SimpleName(narrowedOtherName))),
                        new ReturnStmt(expr),
                        new ReturnStmt(new BooleanLiteralExpr(false))));

        clz.addMethod("equals", Modifier.Keyword.PUBLIC)
                .setType("boolean")
                .addAnnotation(mkAtOverride())
                .setParameters(new NodeList<>(new Parameter(new ClassOrInterfaceType(null, "java.lang.Object"), otherParamName)))
                .setBody(new BlockStmt(new NodeList<>(stmt)));
    }

    @NonNull
    private static MarkerAnnotationExpr mkAtOverride() {
        return new MarkerAnnotationExpr("java.lang.Override");
    }

    private FieldDeclaration mkPropertyField(String propName,
                                             boolean required,
                                             Type propType) {
        var fieldName = fieldName(propName);
        FieldDeclaration fieldDeclaration = new FieldDeclaration();
        fieldDeclaration.addAnnotation(mkNullableAnnotation(required));
        VariableDeclarator variable = new VariableDeclarator(propType, fieldName);
        fieldDeclaration.getVariables().add(variable);
        fieldDeclaration.setModifiers(Modifier.Keyword.PRIVATE);
        return fieldDeclaration;
    }

    // @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    @NonNull
    private static NormalAnnotationExpr mkAtJsonSetter() {
        return new NormalAnnotationExpr(new Name("com.fasterxml.jackson.annotation.JsonSetter"),
                NodeList.nodeList(new MemberValuePair("nulls", new FieldAccessExpr(new TypeExpr(
                        new ClassOrInterfaceType(null, "com.fasterxml.jackson.annotation.Nulls")), "SKIP"))));
    }

    // @com.fasterxml.jackson.annotation.JsonProperty(value = "name", required = )
    private static NormalAnnotationExpr mkAtJsonProperty(String propName, boolean required) {
        NodeList<MemberValuePair> jsonPropertyMembers = NodeList.nodeList(
                new MemberValuePair("value", new StringLiteralExpr(propName)));
        if (required) {
            jsonPropertyMembers.add(new MemberValuePair("required", new BooleanLiteralExpr(true)));
        }
        return new NormalAnnotationExpr(
                new Name("com.fasterxml.jackson.annotation.JsonProperty"),
                jsonPropertyMembers);
    }

    @NonNull
    private AnnotationExpr mkNullableAnnotation(boolean required) {
        return new MarkerAnnotationExpr(required ? nonNullAnnotation : nullableAnnotation);
    }

    private MethodDeclaration mkPropertyGetterMethod(@Nullable String description,
                                                     String propName,
                                                     Type propType,
                                                     boolean required) {
        String getterName = getterName(propName);
        String fieldName = fieldName(propName);

        if (description == null) {
            description = "Return the " + propName + ".\n";
        }
        description += "\n@return The value of this object's " + propName + ".\n";

        MethodDeclaration methodDeclaration = new MethodDeclaration();
        methodDeclaration.setJavadocComment(description);
        methodDeclaration.setModifiers(Modifier.Keyword.PUBLIC);
        methodDeclaration.addAnnotation(mkNullableAnnotation(required));
        methodDeclaration.addAnnotation(mkAtJsonProperty(propName, required));
        methodDeclaration.setType(propType);
        methodDeclaration.setName(getterName);
        methodDeclaration.setBody(new BlockStmt(new NodeList<>(new ReturnStmt(new FieldAccessExpr(new ThisExpr(), fieldName)))));
        return methodDeclaration;
    }

    @NonNull
    private static String fieldName(String propName) {
        return quoteMember(propName);
    }

    private MethodDeclaration mkPropertySetterMethod(@Nullable String description,
                                                     String propName,
                                                     Type propType, boolean required) {
        var fieldName = fieldName(propName);

        if (description == null) {
            description = "Set the " + propName + ".\n";
        }
        description += "\n @param " + fieldName + " The new value for this object's " + propName + ".\n";

        MethodDeclaration methodDeclaration = new MethodDeclaration();
        methodDeclaration.setJavadocComment(description);
        methodDeclaration.setModifiers(Modifier.Keyword.PUBLIC);
        methodDeclaration.setType(new VoidType());
        methodDeclaration.setName(setterName(propName));
        Parameter parameter = new Parameter(propType, fieldName).addAnnotation(mkNullableAnnotation(required));
        methodDeclaration
                .setParameters(new NodeList<>(parameter))
                .setBody(new BlockStmt(new NodeList<>(new ExpressionStmt(new AssignExpr(
                        new FieldAccessExpr(new ThisExpr(), fieldName),
                        required ? new MethodCallExpr("java.util.Objects.requireNonNull", new NameExpr(fieldName)) : new NameExpr(fieldName),
                        AssignExpr.Operator.ASSIGN)))));

        return methodDeclaration;
    }

    @NonNull
    private static String setterName(String propName) {
        return fieldName(propName);
    }

    @NonNull
    private static String getterName(String propName) {
        return fieldName(propName);
    }

    private static String quoteMember(String memberName) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < memberName.length(); i++) {
            int codePoint = memberName.codePointAt(i);
            if (i == 0 ? Character.isJavaIdentifierStart(codePoint)
                    : Character.isJavaIdentifierPart(codePoint)) {
                builder.appendCodePoint(codePoint);
            }
            else {
                builder.append("_");
            }
        }
        String ident = builder.toString();
        return switch (ident) {
            // TODO check we got them all
            case "null", "boolean", "int", "byte", "short", "long", "float", "double", "char" -> ident + "_";
            case "class", "interface", "enum", "public", "private", "protected", "final", "transient", "package", "module" -> ident + "_";
            case "return", "break", "continue", "for", "while", "switch", "case", "default", "if", "else" -> ident + "_";
            default -> ident;
        };
    }

    private class CodeGenVisitor extends SchemaObject.Visitor {
        private final SchemaInput input;
        private final ArrayList<CompilationUnit> result;

        CodeGenVisitor(
                       SchemaInput input,
                       ArrayList<CompilationUnit> result) {
            this.input = input;
            this.result = result;
        }

        @Override
        public void enterSchema(URI base, String path, String keyword, SchemaObject schema) {
            if (schema.getRef() == null) {
                if (isJunctorChild(keyword)) {
                    return;
                }
                // We don't generate code for a ref, on the basis that we've already generated code for it
                // (e.g. when we visited the schemas in /definitions).
                // This means even if multiple refs point to the same thing, that thing should only get code gen'd once.
                CompilationUnit value = genDecl(input.pkg(), schema, path, base);
                if (value != null) {
                    result.add(value);
                }
            }
        }

        /**
         * Is the given path a child of {@code allOf}, {@code oneOf}, {@code anyOf} or {@code not}
         * @param keyword The keyword
         * @return true the schema at this path is a child of a logical junctor
         */
        private boolean isJunctorChild(String keyword) {
            return "oneOf".equals(keyword)
                    || "allOf".equals(keyword)
                    || "anyOf".equals(keyword)
                    || "not".equals(keyword);

        }
    }

}
