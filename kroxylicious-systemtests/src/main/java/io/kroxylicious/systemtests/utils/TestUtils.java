/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.systemtests.Environment;

import info.schnatterer.mobynamesgenerator.MobyNamesGenerator;

/**
 * The type Test utils.
 */
public class TestUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String USER_PATH = System.getProperty("user.dir");
    private static final Pattern IMAGE_PATTERN_FULL_PATH = Pattern.compile("^(?<registry>[^/]*)/(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");
    private static final Pattern IMAGE_PATTERN = Pattern.compile("^(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");

    private TestUtils() {
    }

    /**
     * Gets default posix file permissions.
     *
     * @return the default posix file permissions
     */
    public static FileAttribute<Set<PosixFilePermission>> getDefaultPosixFilePermissions() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    }

    /**
     * Gets resources URI.
     *
     * @param fileName the file name
     * @return the resources URI
     */
    public static URI getResourcesURI(String fileName) {
        URI overrideFile;
        var resource = TestUtils.class.getClassLoader().getResource(fileName);
        try {
            if (resource == null) {
                throw new IllegalArgumentException("Cannot find resource " + fileName + " on classpath");
            }
            overrideFile = resource.toURI();
        }
        catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot determine file system path for " + resource, e);
        }
        return overrideFile;
    }

    /**
     * Is valid json.
     *
     * @param value the value
     * @return the boolean
     */
    public static boolean isValidJson(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        try {
            OBJECT_MAPPER.readTree(value);
        }
        catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * Gets random suffix to be added to a pod Name.
     *
     * @return the random pod name
     */
    public static String getRandomPodNameSuffix() {
        return MobyNamesGenerator.getRandomName().replace("_", "-");
    }

    /**
     * The method to configure docker image to use proper docker registry, docker org and docker tag.
     * @param image Image that needs to be changed
     * @return Updated docker image with a proper registry, org, tag
     */
    public static String changeOrgAndTag(String image) {
        Matcher matcher = IMAGE_PATTERN_FULL_PATH.matcher(image);
        if (matcher.find()) {
            String registry = setImageProperties(matcher.group("registry"), Environment.KROXY_REGISTRY, Environment.KROXY_REGISTRY_DEFAULT);
            String org = setImageProperties(matcher.group("org"), Environment.KROXY_ORG, Environment.KROXY_ORG_DEFAULT);

            return registry + "/" + org + "/" + matcher.group("image") + ":" + buildTag(matcher.group("tag"));
        }
        matcher = IMAGE_PATTERN.matcher(image);
        if (matcher.find()) {
            String org = setImageProperties(matcher.group("org"), Environment.KROXY_ORG, Environment.KROXY_ORG_DEFAULT);

            return Environment.KROXY_REGISTRY + "/" + org + "/" + matcher.group("image") + ":" + buildTag(matcher.group("tag"));
        }
        return image;
    }

    private static String setImageProperties(String current, String envVar, String defaultEnvVar) {
        if (!envVar.equals(defaultEnvVar) && !current.equals(envVar)) {
            return envVar;
        }
        return current;
    }

    private static String buildTag(String currentTag) {
        if (!currentTag.equals(Environment.KROXY_TAG) && !Environment.KROXY_TAG_DEFAULT.equals(Environment.KROXY_TAG)) {
            currentTag = Environment.KROXY_TAG;
        }
        return currentTag;
    }

    /**
     * Gets json file content.
     *
     * @param fileName the file name
     * @return the json file content
     */
    public static String getJsonFileContent(String fileName) {
        try {
            return OBJECT_MAPPER.readTree(new File(Path.of(getResourcesURI(fileName)).toString())).toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
