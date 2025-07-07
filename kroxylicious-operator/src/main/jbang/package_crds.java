///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.yaml:snakeyaml:2.2
//JAVA 21+

import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.yaml.snakeyaml.Yaml;
import static java.lang.System.*;

public class package_crds {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            out.println("Usage: jbang package_crds.java SOURCEDIR TARGETDIR");
            System.exit(1);
        }

        var sourceDir = Path.of(args[0]);
        if (!Files.exists(sourceDir)) {
            out.println("[package_crds.java] SOURCEDIR '" + sourceDir + "' does not exist");
            System.exit(1);
        }
        var targetDir = Path.of(args[1]);

        Files.createDirectories(targetDir);
        var yaml = new Yaml();

        try (var stream = Files.list(sourceDir)) {
            stream.filter(path -> path.toString().endsWith(".yaml") || path.toString().endsWith(".yml"))
                    .forEach(path -> {
                        try (var reader = Files.newBufferedReader(path)) {
                            String singular;
                            String kind;
                            if (yaml.load(reader) instanceof Map<?, ?> data) {
                                kind = kind(data);
                                singular = singular(data);
                            } else {
                                throw new IOException("YAML was not an object");
                            }
                            var target = targetDir.resolve("00." + kind + "." + singular + ".yaml");
                            Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
                            System.out.println("Copied to " + target);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException("With file " + path, e);
                        }
                    });
        }
    }

    private static String kind(Map<?, ?> data) throws IOException {
        if (data.get("kind") instanceof String kind) {
            return kind;
        }
        throw new IOException("YAML lacks .kind, or it is not an string");
    }

    private static String singular(Map<?, ?> data) throws IOException {
        if (data.get("spec") instanceof Map<?, ?> spec) {
            if (spec.get("names") instanceof Map<?, ?> names) {
                if (names.get("singular") instanceof String singular) {
                    return singular;
                }
                throw new IOException("YAML lacks .spec.names.singular, or it is not a string");
            }
            throw new IOException("YAML lacks .spec.names, or it is not an object");
        }
        throw new IOException("YAML lacks .spec, or it is not an object");
    }
}