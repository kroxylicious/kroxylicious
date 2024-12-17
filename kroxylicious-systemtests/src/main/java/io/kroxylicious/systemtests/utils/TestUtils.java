/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.kroxylicious.systemtests.Environment;

import info.schnatterer.mobynamesgenerator.MobyNamesGenerator;

/**
 * The type Test utils.
 */
public class TestUtils {
    public static final String USER_PATH = System.getProperty("user.dir");
    private static final Pattern IMAGE_PATTERN_FULL_PATH = Pattern.compile("^(?<registry>[^/]*)/(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");
    private static final Pattern IMAGE_PATTERN = Pattern.compile("^(?<org>[^/]*)/(?<image>[^:]*):(?<tag>.*)$");

    private TestUtils() {
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
}
