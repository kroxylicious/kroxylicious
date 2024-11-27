/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import io.kroxylicious.tools.schema.compiler.FatalException;
import io.kroxylicious.tools.schema.compiler.SchemaCompiler;

@Mojo(name = "compile-schema", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class CompileSchemaMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}")
    private MavenProject project;

    /**
     * The input file or directory to be used for generating sources
     *
     */
    @Parameter(property = "kroxylicious.schema.source", defaultValue = "${basedir}/src/main/schema")
    File source;

    @Parameter(property = "kroxylicious.schema.header")
    File headerFile;

    @Parameter(property = "kroxylicious.schema.dest", defaultValue = "${basedir}/target/generated-sources/java")
    File target;

    @Parameter(defaultValue = "false")
    boolean failOnWarnings = false;

    @Parameter(property = "kroxylicious.schema.existingClasses", required = false)
    Map<String, String> existingClasses = null;

    @Override
    public void execute() throws MojoExecutionException {
        getLog().info("" + this);
        String header = readHeaderFile();
        try {

            SchemaCompiler schemaCompiler = new SchemaCompiler(List.of(source.toPath()), header, existingClasses != null ? existingClasses : Map.of());
            var inputs = schemaCompiler.parse();
            var units = schemaCompiler.gen(inputs);
            schemaCompiler.write(target.toPath(), units);
            if (schemaCompiler.numFatals() > 0) {
                throw new MojoExecutionException("Schema compilation failed with " + schemaCompiler.numFatals() + " fatal errors.");
            }
            else if (schemaCompiler.numErrors() > 0) {
                throw new MojoExecutionException("Schema compilation failed with " + schemaCompiler.numErrors() + " errors.");
            }
            else if (schemaCompiler.numWarnings() > 0) {
                String msg = "Schema compilation completed with " + schemaCompiler.numWarnings() + " warnings.";
                if (failOnWarnings) {
                    throw new MojoExecutionException(msg + " Failing build because failOnWarnings=true.");
                }
                getLog().warn(msg);
            }
        }
        catch (UncheckedIOException | FatalException e) {
            throw new MojoExecutionException(e);
        }
        project.addCompileSourceRoot(target.getAbsolutePath());
    }

    private String readHeaderFile() throws MojoExecutionException {
        String header = null;
        if (headerFile != null) {
            try {
                header = Files.readString(headerFile.toPath());
            }
            catch (IOException e) {
                throw new MojoExecutionException("Header file " + headerFile + " could not be read", e);
            }
        }
        return header;
    }

    @Override
    public String toString() {
        return "CompileSchemaMojo{" +
                "existingClasses=" + existingClasses +
                ", project=" + project +
                ", source=" + source +
                ", headerFile=" + headerFile +
                ", target=" + target +
                ", failOnWarnings=" + failOnWarnings +
                '}';
    }
}
