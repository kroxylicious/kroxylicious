package com.github.tombentley.krpccodegen.maven;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import com.github.tombentley.krpccodegen.main.KrpcGenerator;

@Mojo(name = "generate-messages", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class KrpcGeneratorMojo extends AbstractMojo {

    /**
     * Gives access to the Maven project information.
     */
    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject project;

    @Parameter(required = true)
    private File messageSpecDirectory;

    @Parameter(defaultValue = "*.json")
    private String messageSpecFilter;

    @Parameter(required = true)
    private File templateDirectory;

    @Parameter(required = true)
    private String templateNames;

    @Parameter(defaultValue = "${messageSpecName}.java")
    private String outputFilePattern;

    @Parameter(defaultValue = "${project.build.directory}${file.separator}generated-sources${file.separator}/krpc")
    private File outputDirectory;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        List<String> templates = Stream.of(templateNames.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        KrpcGenerator gen = new KrpcGenerator.Builder()
                .withLogger(new MavenLogger(KrpcGenerator.class.getName(), getLog()))
                .withMessageSpecDir(messageSpecDirectory)
                .withMessageSpecFilter(messageSpecFilter)
                .withTemplateDir(templateDirectory)
                .withTemplateNames(templates)
                .withOutputDir(outputDirectory)
                .withOutputFilePattern(outputFilePattern)
                .build();

        try {
            gen.generate();
        }
        catch (IOException e) {
            throw new MojoExecutionException("Couldn't generate messages", e);
        }

        project.addCompileSourceRoot(outputDirectory.getAbsolutePath());
    }
}
