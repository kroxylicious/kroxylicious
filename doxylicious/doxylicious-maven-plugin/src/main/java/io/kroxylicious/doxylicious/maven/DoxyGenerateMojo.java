/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.maven;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import io.kroxylicious.doxylicious.Base;
import io.kroxylicious.doxylicious.ProcCheckException;
import io.kroxylicious.doxylicious.doc.DocumentationException;
import io.kroxylicious.doxylicious.doc.ProcAsciidocGenerator;
import io.kroxylicious.doxylicious.model.ProcDecl;
import io.kroxylicious.doxylicious.model.Unit;

/**
 * A goal that converts from ProcDecls defined in YAML files
 * (in src/main/proc or some other configurable directory)
 * to asciidoc.
 */
@Mojo(name = "generate", defaultPhase = LifecyclePhase.PROCESS_RESOURCES)
public class DoxyGenerateMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}")
    private MavenProject project;

    /**
     * The input file or directory, containing the public ProcDecl YAMLs to be documented.
     */
    @Parameter(property = "doxylicious.source", defaultValue = "${basedir}/src/main/proc")
    File procsDir;

    /**
     * The input file or directory, containing supporting ProcDecl YAMLs which will not be documented
     * but may be depended upon by those in the given {@code procsDir}.
     */
    @Parameter(property = "doxylicious.test-source", defaultValue = "${basedir}/src/test/proc")
    File testProcsDir;

    /**
     * The input file or directory, containing ProcDecl YAMLs
     */
    @Parameter(property = "doxylicious.target", defaultValue = "${basedir}/target/proc/asciidoc")
    File targetDir;

    private final LoggingErrorReporter errorReporter = new LoggingErrorReporter(getLog());

    @Override
    public void execute()
            throws MojoExecutionException, MojoFailureException {
        try {
            Base base = Base.fromDirectories(List.of(
                    procsDir.toPath(),
                    testProcsDir.toPath()));
            for (var unit : base.allUnits()) {
                if (!unit.sourceName().startsWith(procsDir.toPath().toString())) {
                    continue;
                }
                for (ProcDecl procDecl : unit.procs()) {
                    if (procDecl.notional()) {
                        continue;
                    }
                    generateAsciidoc(unit, procDecl, errorReporter);
                }
            }
        }
        catch (ProcCheckException e) {
            errorReporter.reportError(e.getMessage());
        }

        if (errorReporter.numErrors() > 0) {
            throw new MojoFailureException(String.format("There were %s errors during generation.", errorReporter.numErrors()));
        }
    }

    private void generateAsciidoc(Unit unit,
                                  ProcDecl procDecl,
                                  LoggingErrorReporter errorReporter)
            throws MojoExecutionException {
        try {
            ProcAsciidocGenerator procAsciidocGenerator = new ProcAsciidocGenerator();
            Path relativeUnitPath = procsDir.toPath().relativize(Path.of(unit.sourceName()));
            var outParent = targetDir.toPath().resolve(relativeUnitPath.getParent());
            Files.createDirectories(outParent);
            Path out = outParent.resolve("proc_" + procDecl.id() + ".adoc");
            try (var writer = new FileWriter(out.toFile(), StandardCharsets.UTF_8)) {
                procAsciidocGenerator.renderProc(unit, procDecl, errorReporter, writer);
            }
        }
        catch (IOException | DocumentationException te) {
            throw new MojoExecutionException("Error generating asciidoc for proc '%s' in file %s: %s"
                    .formatted(procDecl.id(), unit.sourceName(), te.getMessage()), te);
        }
    }

}
