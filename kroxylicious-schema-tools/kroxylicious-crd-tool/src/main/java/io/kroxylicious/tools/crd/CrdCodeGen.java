/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.crd;

import java.util.List;

import javax.annotation.Nullable;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.expr.ArrayInitializerExpr;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.BooleanLiteralExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.FieldAccessExpr;
import com.github.javaparser.ast.expr.InstanceOfExpr;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.Name;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.expr.SimpleName;
import com.github.javaparser.ast.expr.SingleMemberAnnotationExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.expr.ThisExpr;
import com.github.javaparser.ast.expr.TypePatternExpr;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.IfStmt;
import com.github.javaparser.ast.stmt.ReturnStmt;
import com.github.javaparser.ast.type.ClassOrInterfaceType;

import io.kroxylicious.tools.crd.model.Crd;
import io.kroxylicious.tools.crd.model.CrdNames;

import edu.umd.cs.findbugs.annotations.NonNull;

public class CrdCodeGen {

    @Nullable
    CompilationUnit genDecl(String pkg, Crd crd) {
        CompilationUnit cu = new CompilationUnit();
        cu.setPackageDeclaration(new PackageDeclaration(new Name(pkg)));

        String kind = crdTypeName(crd.spec().names());
        ClassOrInterfaceDeclaration clz = cu.addClass(kind, Modifier.Keyword.PUBLIC);
        /*
         * TODO @io.fabric8.kubernetes.model.annotation.Version(value = "v1alpha1" , storage = true , served = true)
         * TODO @lombok.ToString()
         * TODO @lombok.EqualsAndHashCode(callSuper = true)
         * TODO @io.sundr.builder.annotations.Buildable(editableEnabled = false, validationEnabled = false, generateBuilderPackage = false, builderPackage = "io.fabric8.kubernetes.api.builder", refs =
         * {
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ObjectMeta.class),
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ObjectReference.class),
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.LabelSelector.class),
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.Container.class),
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.EnvVar.class),
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.ContainerPort.class),
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.Volume.class),
         *
         * @io.sundr.builder.annotations.BuildableReference(io.fabric8.kubernetes.api.model.VolumeMount.class)
         * })
         */
        if (crd.spec() != null) {
            clz.addSingleMemberAnnotation("javax.annotation.processing.Generated", new StringLiteralExpr(getClass().getName()));
            clz.addSingleMemberAnnotation("io.fabric8.kubernetes.model.annotation.Group", new StringLiteralExpr(crd.spec().group()));
            if (crd.spec().names() != null) {
                clz.addSingleMemberAnnotation("io.fabric8.kubernetes.model.annotation.Singular", new StringLiteralExpr(crd.spec().names().singular()));
                clz.addSingleMemberAnnotation("io.fabric8.kubernetes.model.annotation.Plural", new StringLiteralExpr(crd.spec().names().plural()));
                if (!crd.spec().names().shortNames().isEmpty()) {
                    clz.addAnnotation(mkAtShortNames(crd.spec().names().shortNames()));
                }
            }
        }

        // extends io.fabric8.kubernetes.client.CustomResource<
        // io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec,
        // io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus>
        clz.addExtendedType(new ClassOrInterfaceType(null, "io.fabric8.kubernetes.client.CustomResource")
                .setTypeArguments(new NodeList<>(List.of(
                        mkSpecTypeExpr(pkg, kind),
                        mkStatusTypeExpr(pkg, kind)))));
        // implements io.fabric8.kubernetes.api.model.Namespaced
        if (crd.spec().scope().equalsIgnoreCase("Namespaced")) {
            clz.addImplementedType("io.fabric8.kubernetes.api.model.Namespaced");
        }
        boolean editable = true;
        if (editable) {
            // implements ... io.fabric8.kubernetes.api.builder.Editable<KafkaProxyBuilder>
            clz.addImplementedType(new ClassOrInterfaceType(null, "io.fabric8.kubernetes.api.builder.Editable")
                    .setTypeArguments(mkBuilderTypeExpr(pkg, kind)));
            clz.addMember(mkEditMethod(pkg, kind));
        }

        clz.addMember(mkToStringMethod(crd.spec().names()));
        clz.addMember(mkHashCodeMethod(crd.spec().names()));
        clz.addMember(mkEqualsMethod(pkg, crd.spec().names()));

        return cu;
    }

    private static MethodDeclaration mkEditMethod(String pkg, String kind) {
        return new MethodDeclaration(NodeList.nodeList(Modifier.publicModifier()), mkBuildableTypeExpr(pkg, kind), "edit")
                .addAnnotation("java.lang.Override")
                .setBody(new BlockStmt(NodeList.nodeList(new ReturnStmt(new ObjectCreationExpr(null, mkBuilderTypeExpr(pkg, kind), NodeList.nodeList())))));
    }

    @NonNull
    private static ClassOrInterfaceType mkBuildableTypeExpr(String pkg, String kind) {
        return new ClassOrInterfaceType(null, pkg + "." + buildableTypeName(kind));
    }

    private static String crdTypeName(CrdNames names) {
        return names.kind();
    }

    private static ClassOrInterfaceType mkCrdTypeExpr(String pkg, CrdNames names) {
        return new ClassOrInterfaceType(null, pkg + "." + crdTypeName(names));
    }

    private static MethodDeclaration mkToStringMethod(CrdNames names) {
        var crdTypeName = crdTypeName(names);
        // TODO super.kind, super.metadata
        BinaryExpr expr = new BinaryExpr(new FieldAccessExpr(new ThisExpr(), "status"),
                new StringLiteralExpr("}"),
                BinaryExpr.Operator.PLUS);
        expr = new BinaryExpr(new StringLiteralExpr(", status="),
                expr,
                BinaryExpr.Operator.PLUS);
        expr = new BinaryExpr(new FieldAccessExpr(new ThisExpr(), "spec"),
                expr,
                BinaryExpr.Operator.PLUS);
        BinaryExpr spec = new BinaryExpr(new StringLiteralExpr(crdTypeName + " { spec="),
                expr,
                BinaryExpr.Operator.PLUS);

        return new MethodDeclaration(
                NodeList.nodeList(Modifier.publicModifier()),
                new ClassOrInterfaceType(null, "java.lang.String"),
                "toString")
                .addAnnotation("java.lang.Override")
                .setBody(new BlockStmt(NodeList.nodeList(new ReturnStmt(spec))));
    }

    private static MethodDeclaration mkHashCodeMethod(CrdNames names) {
        // TODO Why are we overriding this, and not just inheriting the super's impl
        return new MethodDeclaration(
                NodeList.nodeList(Modifier.publicModifier()),
                new ClassOrInterfaceType(null, "int"),
                "hashCode")
                .addAnnotation("java.lang.Override")
                .setBody(new BlockStmt(NodeList.nodeList(new ReturnStmt(
                        new MethodCallExpr("java.util.Objects.hash",
                                new FieldAccessExpr(new ThisExpr(), "spec"),
                                new FieldAccessExpr(new ThisExpr(), "status"))))));
    }

    private static MethodDeclaration mkEqualsMethod(String pkg, CrdNames names) {
        // TODO Why are we overriding this, and not just inheriting the super's impl
        String var = "other" + crdTypeName(names);
        MethodCallExpr specEqual = new MethodCallExpr("java.util.Objects.equals",
                new FieldAccessExpr(new ThisExpr(), "spec"),
                new FieldAccessExpr(new NameExpr(var), "spec"));
        MethodCallExpr statusEqual = new MethodCallExpr("java.util.Objects.equals",
                new FieldAccessExpr(new ThisExpr(), "status"),
                new FieldAccessExpr(new NameExpr(var), "status"));
        var bothEqual = new BinaryExpr(specEqual, statusEqual, BinaryExpr.Operator.AND);

        var ident = new BinaryExpr(new ThisExpr(), new NameExpr("other"), BinaryExpr.Operator.EQUALS);

        return new MethodDeclaration(
                NodeList.nodeList(Modifier.publicModifier()),
                new ClassOrInterfaceType(null, "boolean"),
                "equals")
                .addAnnotation("java.lang.Override")
                .addParameter(new ClassOrInterfaceType(null, "java.lang.Object"), "other")
                .setBody(new BlockStmt(NodeList.nodeList(new IfStmt(ident, new ReturnStmt(new BooleanLiteralExpr(true)),
                        new IfStmt(new InstanceOfExpr(new NameExpr("other"), mkCrdTypeExpr(pkg, names),
                                new TypePatternExpr(new NodeList<>(), mkCrdTypeExpr(pkg, names), new SimpleName(var))),
                                new ReturnStmt(bothEqual),
                                new ReturnStmt(new BooleanLiteralExpr(false)))))));
    }

    @NonNull
    private static String statusTypeName(String kind) {
        return kind + "Status";
    }

    @NonNull
    private static ClassOrInterfaceType mkStatusTypeExpr(String pkg, String kind) {
        return new ClassOrInterfaceType(null, pkg + "." + statusTypeName(kind));
    }

    @NonNull
    private static String specTypeName(String kind) {
        return kind + "Spec";
    }

    @NonNull
    private static ClassOrInterfaceType mkSpecTypeExpr(String pkg, String kind) {
        return new ClassOrInterfaceType(null, pkg + "." + specTypeName(kind));
    }

    @NonNull
    private static String buildableTypeName(String kind) {
        return kind + "Builder";
    }

    @NonNull
    private static ClassOrInterfaceType mkBuilderTypeExpr(String pkg, String kind) {
        return new ClassOrInterfaceType(null, pkg + "." + buildableTypeName(kind));
    }

    @NonNull
    private static SingleMemberAnnotationExpr mkAtShortNames(List<String> shortNames) {
        List<Expression> list = shortNames.stream()
                .map(
                        name -> (Expression) new StringLiteralExpr(null, name))
                .toList();
        return new SingleMemberAnnotationExpr(StaticJavaParser.parseName("io.fabric8.kubernetes.model.annotation.ShortNames"),
                new ArrayInitializerExpr(new NodeList<>(list)));
    }

}
