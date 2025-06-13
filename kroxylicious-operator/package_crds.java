/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.yaml:snakeyaml:2.2

import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.yaml.snakeyaml.Yaml;

public class package_crds {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: jbang package_crds.java SOURCEDIR TARGETDIR");
            System.exit(1);
        }

        var sourceDir = Path.of(args[0]);
        if (!Files.exists(sourceDir)) {
            System.out.println("[package_crds.java] SOURCEDIR '" + sourceDir + "' does not exist");
            System.exit(1);
        }
        var targetDir = Path.of(args[1]);

        Files.createDirectories(targetDir);
        var yaml = new Yaml();

        try (var stream = Files.list(sourceDir)) {
            stream.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                    .forEach(p -> {
                        try (var reader = Files.newBufferedReader(p)) {
                            var data = (Map<?, ?>) yaml.load(reader);
                            var kind = data.get("kind");
                            var spec = (Map<?, ?>) data.get("spec");
                            var names = (Map<?, ?>) spec.get("names");
                            var singular = names.get("singular");
                            var target = targetDir.resolve("00." + kind + "-" + singular + ".yaml");
                            Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING);
                            System.out.println("Copied to " + target);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}