/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import javax.tools.StandardLocation;

/**
 * Captures {@code @ApiVersion} metadata from plugin interfaces at compile time,
 * writing it to resource files for runtime version mismatch detection.
 */
@SupportedAnnotationTypes("io.kroxylicious.proxy.plugin.Plugin")
public class ApiVersionProcessor extends AbstractProcessor {

    static final String RESOURCE_PREFIX = "META-INF/kroxylicious/api-version/";
    private static final String API_VERSION_ANNOTATION = "io.kroxylicious.proxy.plugin.ApiVersion";

    private final Map<String, Map<String, String>> implToVersions = new LinkedHashMap<>();

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            writeResourceFiles();
            return false;
        }

        for (TypeElement annotation : annotations) {
            for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
                if (element instanceof TypeElement typeElement) {
                    processPluginImpl(typeElement);
                }
            }
        }

        return false;
    }

    private void processPluginImpl(TypeElement implElement) {
        String implFqcn = implElement.getQualifiedName().toString();
        Map<String, String> versions = new LinkedHashMap<>();
        findApiVersions(implElement, versions);

        if (versions.isEmpty()) {
            processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    "Plugin implementation does not implement any interface annotated with @ApiVersion",
                    implElement);
        }
        else {
            implToVersions.put(implFqcn, versions);
        }
    }

    private void findApiVersions(TypeElement typeElement, Map<String, String> versions) {
        for (TypeMirror supertype : processingEnv.getTypeUtils().directSupertypes(typeElement.asType())) {
            if (supertype instanceof DeclaredType declaredType
                    && declaredType.asElement() instanceof TypeElement superElement) {
                String version = getApiVersion(superElement);
                if (version != null) {
                    versions.put(superElement.getQualifiedName().toString(), version);
                }
                findApiVersions(superElement, versions);
            }
        }
    }

    private String getApiVersion(TypeElement element) {
        for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
            TypeElement annotationType = (TypeElement) mirror.getAnnotationType().asElement();
            if (API_VERSION_ANNOTATION.equals(annotationType.getQualifiedName().toString())) {
                for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry : mirror.getElementValues().entrySet()) {
                    if ("value".equals(entry.getKey().getSimpleName().toString())) {
                        return entry.getValue().getValue().toString();
                    }
                }
            }
        }
        return null;
    }

    private void writeResourceFiles() {
        for (Map.Entry<String, Map<String, String>> entry : implToVersions.entrySet()) {
            String implFqcn = entry.getKey();
            Map<String, String> versions = entry.getValue();

            try {
                var fileObject = processingEnv.getFiler().createResource(
                        StandardLocation.CLASS_OUTPUT, "", RESOURCE_PREFIX + implFqcn);
                try (Writer writer = fileObject.openWriter()) {
                    for (Map.Entry<String, String> v : versions.entrySet()) {
                        writer.write(v.getKey() + ":" + v.getValue() + "\n");
                    }
                }
            }
            catch (IOException e) {
                processingEnv.getMessager().printMessage(
                        Diagnostic.Kind.ERROR,
                        "Failed to write API version resource file for " + implFqcn + ": " + e.getMessage());
            }
        }
    }
}
