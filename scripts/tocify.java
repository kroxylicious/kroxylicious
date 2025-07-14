///usr/bin/env jbang "$0" "$@" ; exit $?
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
//DEPS org.jsoup:jsoup:1.20.1
//JAVA 17+

import static java.lang.System.*;
import org.jsoup.*;
import org.jsoup.nodes.*;
import java.io.*;
import java.nio.file.*;

public class tocify {

    public static void main(String... args) throws Exception {
        if (args.length != 3) {
            err.println("""
Usage: hello.java SOURCE TOC TOCLESS
Split the Asciidoctor-generated SOURCE HTML file into TOC and TOCLESS
suitable for use with a Jekyll layout.
""");
            exit(1);
        }
        // sourcePath = index.html
        // tocPath = toc.txt
        // toclessPath = content.txt
        // TODO ?generate index.html (need to know the title, description, release version and ideally some tags)
        // TODO copy the directory structure
        Path sourceDirPath = Path.of(args[0]);
        try (var stream = Files.walk(sourceDirPath)) {
            stream.forEach((Path path) -> {
                if (Files.isRegularFile(path)
                        && path.getFileName().toString().equals("index.html")) {
                    System.out.println(path.toAbsolutePath());
                    if (splitOutToc(path,
                            path.getParent().resolve(Path.of(args[1])),
                            path.getParent().resolve(Path.of(args[2])))) {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }

            });
        }


    }

    static boolean splitOutToc(Path sourcePath, Path tocPath, Path toclessPath) {
        try {
            // Parse the SOURCE
            Document doc = Jsoup.parse(sourcePath, "UTF-8");
            // Find the toc by it's ID
            var toc = doc.getElementById("toc");
            if (toc == null) {
                return false;
            }
            // Remove the toc from the doc
            toc.remove();
            // Drop the "Table of Content" title
            toc.getElementById("toctitle").remove();
            // Write the two nodes
            writeRaw(toc, tocPath);
            writeRaw(doc, toclessPath);
            return true;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    
    static void writeRaw(Node node, Path path) throws IOException {
        // Jekyll/Liquid doesn't have a way of {% include ... %} which
        // *prevents* the included file processing as a liquid template
        // Bracketing with {% raw %}/{% endraw %} is about the best we can do
        // to prevent evaluation, but an {% endraw %} in the HMTL would be
        // enough to break it ☹
        try (var writer = Files.newBufferedWriter(path)) {
            writer.append("{% raw %}\n");
            writer.append(node.toString());
            writer.append("\n{% endraw %}\n");
            writer.flush();
        }    
    }
}
