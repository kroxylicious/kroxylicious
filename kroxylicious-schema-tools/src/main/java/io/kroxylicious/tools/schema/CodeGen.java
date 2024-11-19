/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.expr.ArrayInitializerExpr;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.BooleanLiteralExpr;
import com.github.javaparser.ast.expr.ClassExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.InstanceOfExpr;
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
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.type.Type;

import io.kroxylicious.tools.schema.model.SchemaObject;
import io.kroxylicious.tools.schema.model.XKubeListType;

import edu.umd.cs.findbugs.annotations.NonNull;

public class CodeGen {

    Type genTypeName(String pkg, SchemaObject schema) {
        List<String> type = schema.type();
        if (type == null) {
            type = List.of();
        }
        if (type.size() == 1) {
            switch (type.get(0)) {
                case "null":
                    return new ClassOrInterfaceType(null, "whatever.runtime.Null");
                case "boolean":
                    return new ClassOrInterfaceType(null, "java.lang.Boolean");
                case "integer":
                    return new ClassOrInterfaceType(null, "java.lang.Long");
                case "number":
                    return new ClassOrInterfaceType(null, "java.lang.Double");
                case "string":
                    return new ClassOrInterfaceType(null, "java.lang.String");
                case "array":
                    // TODO pkg needn't be the same as this type
                    SchemaObject itemSchema = schema.items().get(0);
                    var itemType = genTypeName(pkg, itemSchema);
                    XKubeListType xKubeListType = schema.xKubernetesListType();
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
                        List<String> keyPropertyNames = schema.xKubernetesListMapKeys();
                        if (keyPropertyNames == null
                                || keyPropertyNames.size() == 0) {
                            throw new InvalidSchemaException("'x-kubernetes-list-map-keys' property is required when 'x-kubernetes-list-type: map'");
                        }
                        if (keyPropertyNames.size() == 1) {
                            SchemaObject keySchema = itemSchema.properties().get(keyPropertyNames.get(0));
                            var keyType = genTypeName(pkg, keySchema);
                            return new ClassOrInterfaceType(null, new SimpleName("java.util.Map"),
                                    new NodeList<>(keyType, itemType));
                        }
                        else {
                            // x-kubernetes-list-map-keys=['foo', 'bar'] should result in an inner class to represent the compound key
                            throw new InvalidSchemaException("'x-kubernetes-list-map-keys' property with multiple values is not yet supported");
                        }
                    }
                    else {
                        throw new InvalidSchemaException("Unsupported 'x-kubernetes-list-type': " + xKubeListType);
                    }

                case "object":
                    // TODO Java class or record
                    // TODO or Map or ObjectNode if x-kubernetes-preserve-unknown-keys
                    return new ClassOrInterfaceType(null, pkg + "." + className(schema));
                default:
                    throw new InvalidSchemaException("Unsupported type: " + type.get(0));
            }
        }
        else {
            if (type.isEmpty()) {
                // unconstrained => union of all types
                return new ClassOrInterfaceType(null, "java.lang.Object");
            }
            return new ClassOrInterfaceType(null, "java.lang.Object");
        }
    }

    /**
     * Generate a type declaration for the given schema, or null if the type is declared externally
     *
     * @param pkg
     * @param schema
     */
    @Nullable
    CompilationUnit genDecl(String pkg, SchemaObject schema) {
        List<String> type = schema.type();
        if (type.size() == 1) {
            switch (type.get(0)) {
                case "object":
                    // TODO Java class or record
                    // TODO or Map or ObjectNode
                    return genClass(pkg, schema);
                case "array", "string", "integer", "number", "boolean", "null":
                    return null;
                default:
                    throw new InvalidSchemaException("'type' property in schema object has unsupported value '" +
                            type.get(0) + "' supported values are: 'null', 'boolean', 'integer', 'number', 'string', 'array', 'object'.");
            }
        }
        else {
            throw new UnsupportedOperationException("Can't handle union types yet");
        }
    }

    @NonNull
    private CompilationUnit genClass(String pkg, SchemaObject schema) {
        assert (schema.type().equals(List.of("object")));
        Map<String, SchemaObject> properties = schema.properties() == null ? Map.of() : schema.properties();
        CompilationUnit cu = new CompilationUnit();
        cu.setPackageDeclaration(new PackageDeclaration(new Name(pkg)));
        // TODO annotations
        ClassOrInterfaceDeclaration clz = cu.addClass(className(schema),
                Modifier.Keyword.PUBLIC);
        if (schema.description() != null) {
            clz.setJavadocComment(schema.description());
        }

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
            var propSchema = entry.getValue();
            var propType = genTypeName(pkg, propSchema);
            addPropertyField(clz, propName, propType);
        }
        for (var entry : properties.entrySet()) {
            String propName = entry.getKey();
            var propSchema = entry.getValue();
            var propType = genTypeName(pkg, propSchema);
            addPropertyGetter(clz, propSchema.description(), propName, propType);
            addPropertySetter(clz, propSchema.description(), propName, propType);
        }

        addToStringMethod(clz, properties);
        addHashCodeMethod(clz, properties);
        addEqualsMethod(clz, properties);
        return cu;
    }

    @NonNull
    private static String className(SchemaObject schema) {
        if (schema.javaType() != null) {
            return schema.javaType();
        }
        else {
            return schema.id().replaceAll("\\.(ya?ml|json)$", "");
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
                .addAnnotation("java.lang.Override")
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
                .addAnnotation("java.lang.Override")
                .setBody(new BlockStmt(new NodeList<>(new ReturnStmt(new MethodCallExpr("java.util.Objects.hash")
                        .setArguments(args)))));
    }

    private static void addEqualsMethod(ClassOrInterfaceDeclaration clz, Map<String, SchemaObject> properties) {
        // if (this == other) {
        // return true;
        // } else if (other instanceof Bob otherBob) {
        // return Objects.equals(this.foo, otherBob.foo)
        // && Objects.equals(this.bar, otherBob.bar)
        // && ... // for each property;
        // } else { return false; }
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
                        new TypePatternExpr(new NodeList<>(), new ClassOrInterfaceType(null, className), new SimpleName(narrowedOtherName))),
                        new ReturnStmt(expr),
                        new ReturnStmt(new BooleanLiteralExpr(false))));

        clz.addMethod("equals", Modifier.Keyword.PUBLIC)
                .setType("boolean")
                .addAnnotation("java.lang.Override")
                .setParameters(new NodeList<>(new Parameter(new ClassOrInterfaceType(null, "java.lang.Object"), otherParamName)))
                .setBody(new BlockStmt(new NodeList<>(stmt)));
    }

    private static void addPropertyField(ClassOrInterfaceDeclaration clz, String propName, Type propType) {
        // TODO initializer? How work with jackson
        var fieldName = fieldName(propName);
        FieldDeclaration fieldDeclaration = clz.addField(
                propType,
                fieldName,
                Modifier.Keyword.PRIVATE);
        // @com.fasterxml.jackson.annotation.JsonProperty("name")
        // @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
        fieldDeclaration.addAnnotation(new SingleMemberAnnotationExpr(new Name("com.fasterxml.jackson.annotation.JsonProperty"), new StringLiteralExpr(propName)));
        fieldDeclaration.addAnnotation(new NormalAnnotationExpr(new Name("com.fasterxml.jackson.annotation.JsonSetter"),
                new NodeList<>(new MemberValuePair("nulls", new FieldAccessExpr(new TypeExpr(
                        new ClassOrInterfaceType(null, "com.fasterxml.jackson.annotation.Nulls")), "SKIP")))));
    }

    private static void addPropertyGetter(ClassOrInterfaceDeclaration clz,
                                          @Nullable String description,
                                          String propName,
                                          Type propType) {
        String getterName = getterName(propName);
        String fieldName = fieldName(propName);
        MethodDeclaration methodDeclaration = clz.addMethod(getterName, Modifier.Keyword.PUBLIC);
        if (description != null) {
            methodDeclaration.setJavadocComment(description);
        }
        methodDeclaration
                .setType(propType)
                .setBody(new BlockStmt(new NodeList<>(new ReturnStmt(new FieldAccessExpr(new ThisExpr(), fieldName)))));

    }

    @NonNull
    private static String fieldName(String propName) {
        return quoteMember(propName);
    }

    private static void addPropertySetter(ClassOrInterfaceDeclaration clz,
                                          @Nullable String description,
                                          String propName,
                                          Type propType) {
        var fieldName = fieldName(propName);
        MethodDeclaration methodDeclaration = clz.addMethod(setterName(propName), Modifier.Keyword.PUBLIC);
        if (description != null) {
            methodDeclaration.setJavadocComment(description);
        }
        methodDeclaration
                .setParameters(new NodeList<>(new Parameter(propType, fieldName)))
                .setBody(new BlockStmt(new NodeList<>(new ExpressionStmt(new AssignExpr(
                        new FieldAccessExpr(new ThisExpr(), fieldName),
                        new NameExpr(fieldName),
                        AssignExpr.Operator.ASSIGN)))));
    }

    @NonNull
    private static String setterName(String propName) {
        String propName1 = Character.toUpperCase(propName.charAt(0)) + propName.substring(1);
        return quoteMember("set" + propName1);
    }

    @NonNull
    private static String getterName(String propName) {
        String propName1 = Character.toUpperCase(propName.charAt(0)) + propName.substring(1);
        return quoteMember("get" + propName1);
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
            case "null", "boolean", "int", "byte", "short", "long", "float", "double", "char" -> ident + "_";
            case "class", "interface", "enum", "public", "private", "protected", "final", "transient", "package", "module" -> ident + "_";
            case "return", "break", "continue", "for", "while", "switch", "case", "default", "if", "else" -> ident + "_";
            default -> ident;
        };
    }

    public static void main(String[] a) {

    }
}
