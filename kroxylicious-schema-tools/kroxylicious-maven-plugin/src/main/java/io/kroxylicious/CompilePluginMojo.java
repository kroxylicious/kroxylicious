/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.expr.AnnotationExpr;
import com.github.javaparser.ast.expr.ClassExpr;
import com.github.javaparser.ast.expr.MemberValuePair;
import com.github.javaparser.ast.expr.Name;
import com.github.javaparser.ast.expr.NormalAnnotationExpr;
import com.github.javaparser.ast.expr.SimpleName;
import com.github.javaparser.ast.expr.SingleMemberAnnotationExpr;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.type.ClassOrInterfaceType;

import io.kroxylicious.tools.schema.compiler.Diagnostics;
import io.kroxylicious.tools.schema.compiler.PropertyAnnotator;
import io.kroxylicious.tools.schema.compiler.SchemaCompiler;
import io.kroxylicious.tools.schema.model.SchemaObject;
import io.kroxylicious.tools.schema.model.SchemaType;

@Mojo(name = "compile-plugin", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class CompilePluginMojo extends AbstractCompileSchemaMojo {
    @Override
    protected SchemaCompiler schemaCompiler() throws MojoExecutionException {
        return new SchemaCompiler(List.of(source.toPath()),
                null,
                readHeaderFile(),
                existingClasses != null ? existingClasses : Map.of(),
                List.of(),
                List.of(
                        new PropertyAnnotator() {
                            @Override
                            public List<AnnotationExpr> annotateConstructorParameter(
                                                                                     Diagnostics diagnostics,
                                                                                     String property,
                                                                                     SchemaObject propertySchema) {

                                if (List.of(SchemaType.STRING).equals(propertySchema.getType())
                                        && "plugin-impl-name".equals(propertySchema.getFormat())) {
                                    Object o = propertySchema.getAdditionalProperties().get("plugin-interface-name");
                                    if (o instanceof String interfaceName) {
                                        return List.of(new SingleMemberAnnotationExpr(
                                                new Name("io.kroxylicious.proxy.plugin.PluginImplName"),
                                                new ClassExpr(new ClassOrInterfaceType(null,
                                                        interfaceName))));
                                    }
                                    diagnostics.reportError("`format: plugin-impl-name` requires the `plugin-interface-name` property");
                                }
                                else if (List.of(SchemaType.OBJECT).equals(propertySchema.getType())
                                        && "plugin-impl-config".equals(propertySchema.getFormat())) {
                                    Object o = propertySchema.getAdditionalProperties().get("impl-name-property");
                                    if (o instanceof String implName) {
                                        return List.of(new NormalAnnotationExpr(
                                                new Name("io.kroxylicious.proxy.plugin.PluginImplConfig"),
                                                NodeList.nodeList(
                                                        new MemberValuePair(new SimpleName("implNameProperty"),
                                                                new StringLiteralExpr(implName)))));
                                    }

                                    diagnostics.reportError("`format: plugin-impl-config` requires the `impl-name-property` property");
                                }

                                return List.of();
                            }
                        }));
    }
}
