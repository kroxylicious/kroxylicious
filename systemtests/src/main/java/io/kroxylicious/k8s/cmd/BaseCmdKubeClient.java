/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.k8s.cmd;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.executor.Exec;
import io.kroxylicious.executor.ExecResult;

import static java.util.Arrays.asList;

public abstract class BaseCmdKubeClient<K extends BaseCmdKubeClient<K>> implements KubeCmdClient<K> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseCmdKubeClient.class);

    private static final String APPLY = "apply";
    private static final String DELETE = "delete";

    String namespace = defaultNamespace();

    public abstract String cmd();

    protected static class Context implements AutoCloseable {
        @Override
        public void close() {
        }
    }

    private static final Context NOOP = new Context();

    protected Context defaultContext() {
        return NOOP;
    }

    protected List<String> namespacedCommand(String... rest) {
        return namespacedCommand(asList(rest));
    }

    private List<String> namespacedCommand(List<String> rest) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.add("--namespace");
        result.add(namespace);
        result.addAll(rest);
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K apply(File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(APPLY, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to apply {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K delete(File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(DELETE, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to delete {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteByName(String resourceType, String resourceName) {
        Exec.exec(namespacedCommand(DELETE, resourceType, resourceName));
        return (K) this;
    }

    private Map<File, ExecResult> execRecursive(String subcommand, File[] files, Comparator<File> cmp) {
        Map<File, ExecResult> execResults = new HashMap<>(25);
        for (File f : files) {
            if (f.isFile()) {
                if (f.getName().endsWith(".yaml")) {
                    execResults.put(f, Exec.exec(null, namespacedCommand(subcommand, "-f", f.getAbsolutePath()), 0, false, false));
                }
            }
            else if (f.isDirectory()) {
                File[] children = f.listFiles();
                if (children != null) {
                    Arrays.sort(children, cmp);
                    execResults.putAll(execRecursive(subcommand, children, cmp));
                }
            }
            else if (!f.exists()) {
                throw new RuntimeException(new NoSuchFileException(f.getPath()));
            }
        }
        return execResults;
    }

    @Override
    public String toString() {
        return cmd();
    }
}
