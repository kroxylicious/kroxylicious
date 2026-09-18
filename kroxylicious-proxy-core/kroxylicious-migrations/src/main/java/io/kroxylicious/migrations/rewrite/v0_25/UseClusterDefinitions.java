/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.migrations.rewrite.v0_25;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.openrewrite.ExecutionContext;
import org.openrewrite.FindSourceFiles;
import org.openrewrite.Option;
import org.openrewrite.Preconditions;
import org.openrewrite.Recipe;
import org.openrewrite.SourceFile;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.internal.ListUtils;
import org.openrewrite.internal.StringUtils;
import org.openrewrite.marker.Markers;
import org.openrewrite.style.GeneralFormatStyle;
import org.openrewrite.style.Style;
import org.openrewrite.yaml.MergeYaml;
import org.openrewrite.yaml.MergeYamlVisitor;
import org.openrewrite.yaml.YamlIsoVisitor;
import org.openrewrite.yaml.YamlParser;
import org.openrewrite.yaml.YamlVisitor;
import org.openrewrite.yaml.tree.Yaml;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Migrates Kroxylicious proxy configuration YAML from the inline {@code virtualClusters[].targetCluster} form,
 * deprecated in 0.22.0, to the top-level {@code clusterDefinitions} list introduced in its place.
 * <p>
 * Each migrated virtual cluster contributes one {@code clusterDefinitions} entry named
 * {@code <virtualClusterName>-target}, and its inline definition is replaced by a
 * {@code target: { cluster: <name> }} reference. Definitions are not coalesced: two virtual clusters pointing at
 * the same Kafka cluster yield two {@code clusterDefinitions} entries.
 * <p>
 * Because a proxy configuration file can be named anything and live anywhere, the recipe matches all YAML files by
 * default and relies on a structural check to decide whether a document really is a proxy configuration. Use
 * {@code filePattern} to narrow the search.
 */
public class UseClusterDefinitions extends Recipe {

    private static final String DEFAULT_FILE_PATTERN = "**/*.yaml;**/*.yml";

    private static final String VIRTUAL_CLUSTERS = "virtualClusters";
    private static final String CLUSTER_DEFINITIONS = "clusterDefinitions";
    private static final String TARGET_CLUSTER = "targetCluster";
    private static final String TARGET = "target";
    private static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    private static final String NAME = "name";
    private static final String CLUSTER = "cluster";

    /**
     * Root keys whose presence means the document is something other than a proxy configuration - most obviously a
     * Kubernetes manifest, which may legitimately carry a {@code virtualClusters} key deeper in its {@code spec}.
     */
    private static final Set<String> FOREIGN_ROOT_KEYS = Set.of("apiVersion", "kind");

    @Option(displayName = "File pattern", description = "A glob, or `;`-separated list of globs, selecting the proxy configuration file(s) to migrate. "
            + "Defaults to all YAML files; only documents that structurally look like a Kroxylicious proxy "
            + "configuration are modified.", required = false, example = "**/kroxylicious-config.yaml")
    @Nullable
    private final String filePattern;

    /**
     * Instantiates an instance.
     * <p>
     * This must remain the only public constructor: OpenRewrite's {@code RecipeIntrospectionUtils} selects the
     * constructor to instantiate a recipe with by requiring either exactly one public constructor or a
     * {@code @JsonCreator} annotated one, and it fails at runtime rather than at compile time.
     *
     * @param filePattern glob selecting the configuration file(s), or {@code null} for all YAML files
     */
    public UseClusterDefinitions(@Nullable String filePattern) {
        this.filePattern = filePattern;
    }

    @Override
    public String getDisplayName() {
        return "Use `clusterDefinitions` instead of inline `targetCluster`";
    }

    @Override
    public String getDescription() {
        return "Moves each `virtualClusters[].targetCluster` of a Kroxylicious proxy configuration into a named entry "
                + "in the top-level `clusterDefinitions` list, and replaces the inline definition with a "
                + "`target: { cluster: <name> }` reference.";
    }

    @Override
    public String getInstanceNameSuffix() {
        return filePattern == null ? "" : "in `" + filePattern + "`";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return Preconditions.check(new FindSourceFiles(filePattern == null ? DEFAULT_FILE_PATTERN : filePattern),
                new ClusterDefinitionsVisitor());
    }

    /**
     * Two instances are equal when they were configured with the same file pattern.
     * <p>
     * {@link Recipe} compares by class and name alone, so without this every instance of this recipe would be equal
     * whatever it was told to match. Upstream recipes carrying options include them in the same way.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof UseClusterDefinitions that && Objects.equals(filePattern, that.filePattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePattern);
    }

    /**
     * Rewrites one YAML document at a time. Note that OpenRewrite reuses a single visitor instance across every source
     * file in a cycle, so all state is local to {@link #visitDocument}.
     */
    private static class ClusterDefinitionsVisitor extends YamlIsoVisitor<ExecutionContext> {

        @Override
        // S135: the loop below skips the several kinds of virtual cluster which cannot be migrated. Expressing those
        // guards as a single continue would nest the body three deep, which reads worse than the guards themselves.
        @SuppressWarnings("java:S135")
        public Yaml.Document visitDocument(Yaml.Document document, ExecutionContext ctx) {
            if (!(document.getBlock() instanceof Yaml.Mapping root) || !looksLikeProxyConfiguration(root)) {
                return document;
            }
            Yaml.Mapping.Entry virtualClusters = entry(root, VIRTUAL_CLUSTERS);
            if (virtualClusters == null || !(virtualClusters.getValue() instanceof Yaml.Sequence sequence)) {
                return document;
            }

            String newLine = newLine();
            Set<String> usedNames = existingClusterDefinitionNames(root);
            // keyed by the id of the targetCluster entry being replaced
            Map<UUID, String> plannedNames = new LinkedHashMap<>();
            List<String> renderedDefinitions = new ArrayList<>();

            for (Yaml.Sequence.Entry sequenceEntry : sequence.getEntries()) {
                if (!(sequenceEntry.getBlock() instanceof Yaml.Mapping virtualCluster)) {
                    continue;
                }
                Yaml.Mapping.Entry targetCluster = migratableTargetCluster(virtualCluster);
                String virtualClusterName = scalarValue(entry(virtualCluster, NAME));
                if (targetCluster == null || virtualClusterName == null) {
                    continue;
                }
                String clusterDefinitionName = uniqueName(virtualClusterName + "-target", usedNames);
                String rendered = renderClusterDefinition(clusterDefinitionName, (Yaml.Mapping) targetCluster.getValue(), newLine);
                if (rendered == null) {
                    continue;
                }
                usedNames.add(clusterDefinitionName);
                plannedNames.put(targetCluster.getId(), clusterDefinitionName);
                renderedDefinitions.add(rendered);
            }

            if (plannedNames.isEmpty()) {
                return document;
            }

            Yaml incoming = parseClusterDefinitions(CLUSTER_DEFINITIONS + ":" + newLine + String.join(newLine, renderedDefinitions));
            if (incoming == null) {
                // the extracted configuration did not round-trip through the parser, so make no change at all
                return document;
            }

            Set<UUID> inlineEntries = entriesWithoutLeadingLineBreak(incoming);
            Yaml.Document withReferences = document.withBlock(replaceWithReferences(root, virtualClusters, sequence, plannedNames, newLine));
            // mirrors MergeYaml's own handling of the `$` (document root) key
            Yaml.Block block = withReferences.getBlock();
            Yaml.Block merged = (Yaml.Block) new MergeYamlVisitor<ExecutionContext>(block,
                    incoming,
                    false,
                    NAME,
                    MergeYaml.InsertMode.Before,
                    VIRTUAL_CLUSTERS).visitNonNull(block, ctx, getCursor());
            merged = repairBlockScalarSiblings(merged, inlineEntries);
            return withReferences.withBlock(restoreLeadingPrefix(merged, root.getEntries().get(0), newLine));
        }

        /**
         * The ids of the mapping entries of the given tree whose prefix carries no line break.
         */
        private static Set<UUID> entriesWithoutLeadingLineBreak(Yaml tree) {
            Set<UUID> ids = new HashSet<>();
            new YamlIsoVisitor<Set<UUID>>() {

                @Override
                public Yaml.Mapping.Entry visitMappingEntry(Yaml.Mapping.Entry entry, Set<UUID> accumulator) {
                    if (indexOfLineBreak(entry.getPrefix()) < 0) {
                        accumulator.add(entry.getId());
                    }
                    return super.visitMappingEntry(entry, accumulator);
                }
            }.visit(tree, ids);
            return ids;
        }

        /**
         * Undoes the line break {@link MergeYamlVisitor} prepends to the prefix of an entry that followed a block
         * scalar in the inserted YAML.
         * <p>
         * A block scalar holds the line break which terminates it at the end of its own value, so the entry after it
         * legitimately has a prefix of indentation alone. {@code MergeYamlVisitor} rebuilds prefixes on the assumption
         * that every entry begins on a line of its own, which would emit a blank line here. Only the entries which
         * arrived without a line break are repaired, so a blank line the author wrote is left alone.
         */
        private static Yaml.Block repairBlockScalarSiblings(Yaml.Block merged, Set<UUID> arrivedWithoutLineBreak) {
            if (arrivedWithoutLineBreak.isEmpty()) {
                return merged;
            }
            return (Yaml.Block) new YamlIsoVisitor<Integer>() {

                @Override
                public Yaml.Mapping visitMapping(Yaml.Mapping mapping, Integer unused) {
                    Yaml.Mapping visited = super.visitMapping(mapping, unused);
                    List<Yaml.Mapping.Entry> entries = visited.getEntries();
                    return visited.withEntries(ListUtils.map(entries, (index, entry) -> {
                        if (index == 0
                                || !arrivedWithoutLineBreak.contains(entry.getId())
                                || !endsWithLineBreak(entries.get(index - 1).getValue())) {
                            return entry;
                        }
                        return entry.withPrefix(stripLeadingLineBreak(entry.getPrefix()));
                    }));
                }
            }.visitNonNull(merged, 0);
        }

        private static boolean endsWithLineBreak(Yaml.Block value) {
            return value instanceof Yaml.Scalar scalar
                    && (scalar.getStyle() == Yaml.Scalar.Style.LITERAL || scalar.getStyle() == Yaml.Scalar.Style.FOLDED)
                    && (scalar.getValue().endsWith("\n") || scalar.getValue().endsWith("\r"));
        }

        private static String stripLeadingLineBreak(String prefix) {
            if (prefix.startsWith("\r\n")) {
                return prefix.substring(2);
            }
            if (prefix.startsWith("\n") || prefix.startsWith("\r")) {
                return prefix.substring(1);
            }
            return prefix;
        }

        private static int indexOfLineBreak(String prefix) {
            int newline = prefix.indexOf('\n');
            int carriageReturn = prefix.indexOf('\r');
            return newline < 0 ? carriageReturn : newline;
        }

        /**
         * Hands the prefix of the document's original first root entry to whatever entry now precedes it, and gives the
         * displaced entry a plain line break. Without this the line break that follows a {@code ---} separator - which
         * belongs to the prefix of the document's first entry - would be stranded behind the newly inserted
         * {@code clusterDefinitions}.
         */
        private static Yaml.Block restoreLeadingPrefix(Yaml.Block merged, Yaml.Mapping.Entry originalFirst, String newLine) {
            if (!(merged instanceof Yaml.Mapping mapping)
                    || mapping.getEntries().isEmpty()
                    || mapping.getEntries().get(0).getId().equals(originalFirst.getId())) {
                return merged;
            }
            return mapping.withEntries(ListUtils.map(mapping.getEntries(), (index, entry) -> {
                if (index == 0) {
                    return entry.withPrefix(originalFirst.getPrefix());
                }
                return entry.getId().equals(originalFirst.getId()) ? entry.withPrefix(newLine) : entry;
            }));
        }

        /**
         * A document is only migrated if it is a mapping that has a root level {@code virtualClusters} sequence
         * containing at least one virtual cluster in the deprecated form, and that carries none of the root keys which
         * would mark it as some other kind of document.
         */
        private static boolean looksLikeProxyConfiguration(Yaml.Mapping root) {
            if (root.getEntries().stream().anyMatch(e -> FOREIGN_ROOT_KEYS.contains(keyValue(e)))) {
                return false;
            }
            Yaml.Mapping.Entry virtualClusters = entry(root, VIRTUAL_CLUSTERS);
            if (virtualClusters == null || !(virtualClusters.getValue() instanceof Yaml.Sequence sequence)) {
                return false;
            }
            return sequence.getEntries().stream()
                    .map(Yaml.Sequence.Entry::getBlock)
                    .filter(Yaml.Mapping.class::isInstance)
                    .map(Yaml.Mapping.class::cast)
                    .anyMatch(m -> entry(m, NAME) != null && migratableTargetCluster(m) != null);
        }

        /**
         * Returns the {@code targetCluster} entry of the given virtual cluster if, and only if, it is safe to migrate.
         * A virtual cluster that already uses {@code target}, that uses both forms (which the runtime rejects anyway),
         * whose {@code targetCluster} is not a mapping carrying {@code bootstrapServers}, or whose {@code targetCluster}
         * involves anchors or aliases, is left for a human.
         */
        @Nullable
        private static Yaml.Mapping.Entry migratableTargetCluster(Yaml.Mapping virtualCluster) {
            Yaml.Mapping.Entry targetCluster = entry(virtualCluster, TARGET_CLUSTER);
            if (targetCluster == null
                    || entry(virtualCluster, TARGET) != null
                    || !(targetCluster.getValue() instanceof Yaml.Mapping mapping)
                    || entry(mapping, BOOTSTRAP_SERVERS) == null
                    || containsAnchorOrAlias(mapping)) {
                return null;
            }
            return targetCluster;
        }

        private static boolean containsAnchorOrAlias(Yaml.Mapping mapping) {
            return new AnchorOrAliasDetector().reduce(mapping, new AtomicBoolean()).get();
        }

        private static Set<String> existingClusterDefinitionNames(Yaml.Mapping root) {
            Set<String> names = new HashSet<>();
            Yaml.Mapping.Entry clusterDefinitions = entry(root, CLUSTER_DEFINITIONS);
            if (clusterDefinitions != null && clusterDefinitions.getValue() instanceof Yaml.Sequence sequence) {
                for (Yaml.Sequence.Entry sequenceEntry : sequence.getEntries()) {
                    if (sequenceEntry.getBlock() instanceof Yaml.Mapping mapping) {
                        String name = scalarValue(entry(mapping, NAME));
                        if (name != null) {
                            names.add(name);
                        }
                    }
                }
            }
            return names;
        }

        private static String uniqueName(String preferred, Set<String> taken) {
            if (!taken.contains(preferred)) {
                return preferred;
            }
            for (int i = 2;; i++) {
                String candidate = preferred + "-" + i;
                if (!taken.contains(candidate)) {
                    return candidate;
                }
            }
        }

        /**
         * Renders one entry of the top level cluster definition sequence, for example
         * {@code "  - name: demo-target\n    bootstrapServers: localhost:9092"}.
         * <p>
         * The whole {@code targetCluster} mapping is printed rather than being rebuilt node by node. A mapping's own
         * prefix is always empty - the line break and indentation which precede it belong to the prefix of its first
         * entry - so printing the mapping yields a block whose every line is indented by at least the mapping's own
         * indentation, which {@link StringUtils#trimIndent} then removes uniformly. Shifting every line by the same
         * amount is what keeps nested mappings, comments and block scalars intact.
         */
        @Nullable
        private String renderClusterDefinition(String clusterDefinitionName, Yaml.Mapping targetCluster, String newLine) {
            String body;
            if (targetCluster.getOpeningBracePrefix() == null) {
                body = normaliseLineEndings(targetCluster.print(getCursor()));
            }
            else {
                // a flow mapping such as `targetCluster: {bootstrapServers: localhost:9092}` has no line breaks in its
                // entry prefixes, so give each entry a line of its own. The result is a block mapping.
                body = targetCluster.getEntries().stream()
                        .map(e -> normaliseLineEndings(e.print(getCursor())).strip())
                        .collect(Collectors.joining("\n"));
            }
            body = StringUtils.trimIndent(body);
            if (body.isBlank()) {
                return null;
            }
            String indented = body.lines()
                    .map(line -> line.isEmpty() ? line : "    " + line)
                    .collect(Collectors.joining("\n"));
            String rendered = "  - " + NAME + ": " + clusterDefinitionName + "\n" + indented;
            return "\n".equals(newLine) ? rendered : rendered.replace("\n", newLine);
        }

        /**
         * Replaces each planned {@code targetCluster} entry with an equivalent {@code target} reference, keeping the
         * original entry's prefix so that any preceding blank lines or comments are undisturbed.
         */
        private static Yaml.Mapping replaceWithReferences(Yaml.Mapping root,
                                                          Yaml.Mapping.Entry virtualClusters,
                                                          Yaml.Sequence sequence,
                                                          Map<UUID, String> plannedNames,
                                                          String newLine) {
            Yaml.Sequence migrated = sequence.withEntries(ListUtils.map(sequence.getEntries(), sequenceEntry -> {
                if (!(sequenceEntry.getBlock() instanceof Yaml.Mapping virtualCluster)) {
                    return sequenceEntry;
                }
                return sequenceEntry.withBlock(virtualCluster.withEntries(ListUtils.map(virtualCluster.getEntries(), entry -> {
                    String clusterDefinitionName = plannedNames.get(entry.getId());
                    return clusterDefinitionName == null ? entry : toClusterReference(entry, clusterDefinitionName, newLine);
                })));
            }));
            return root.withEntries(ListUtils.map(root.getEntries(),
                    entry -> entry.getId().equals(virtualClusters.getId()) ? entry.withValue(migrated) : entry));
        }

        private static Yaml.Mapping.Entry toClusterReference(Yaml.Mapping.Entry targetCluster, String clusterDefinitionName, String newLine) {
            if (!(targetCluster.getKey() instanceof Yaml.Scalar key)) {
                return targetCluster;
            }
            Yaml.Scalar cluster = new Yaml.Scalar(Tree.randomId(), "", Markers.EMPTY, Yaml.Scalar.Style.PLAIN, null, null, CLUSTER);
            Yaml.Scalar name = new Yaml.Scalar(Tree.randomId(), " ", Markers.EMPTY, Yaml.Scalar.Style.PLAIN, null, null, clusterDefinitionName);
            Yaml.Mapping.Entry clusterEntry = new Yaml.Mapping.Entry(Tree.randomId(),
                    newLine + " ".repeat(childIndentOf(targetCluster)),
                    Markers.EMPTY,
                    cluster,
                    "",
                    name);
            Yaml.Mapping target = new Yaml.Mapping(Tree.randomId(), Markers.EMPTY, null, List.of(clusterEntry), null, null, null);
            return targetCluster.withKey(key.withValue(TARGET)).withValue(target);
        }

        /**
         * The column the children of the given entry should be written at. Preferring the indentation the file already
         * used for {@code targetCluster}'s children honours whatever indent style the author chose.
         */
        private static int childIndentOf(Yaml.Mapping.Entry entry) {
            if (entry.getValue() instanceof Yaml.Mapping mapping && !mapping.getEntries().isEmpty()) {
                String firstChildPrefix = mapping.getEntries().get(0).getPrefix();
                if (firstChildPrefix.indexOf('\n') >= 0 || firstChildPrefix.indexOf('\r') >= 0) {
                    return indentOf(firstChildPrefix);
                }
            }
            return indentOf(entry.getPrefix()) + 2;
        }

        /**
         * The number of characters following the last line break in a prefix, or {@code 0} if it contains none.
         */
        private static int indentOf(String prefix) {
            int lastBreak = Math.max(prefix.lastIndexOf('\n'), prefix.lastIndexOf('\r'));
            return lastBreak < 0 ? 0 : prefix.length() - lastBreak - 1;
        }

        /**
         * Line endings are normalised to {@code \n} while the extracted text is being re-indented, so that the
         * arithmetic does not have to account for two-character line breaks. The document's own line ending is
         * reapplied by {@link #renderClusterDefinition}.
         */
        private static String normaliseLineEndings(String printed) {
            return printed.replace("\r\n", "\n");
        }

        /**
         * The line ending to write. A {@link GeneralFormatStyle} is only attached to a source file when line ending
         * detection has been run over the project, so when there is none the file's own text is consulted rather than
         * assuming {@code \n}.
         */
        private String newLine() {
            Yaml.Documents documents = getCursor().firstEnclosing(Yaml.Documents.class);
            if (documents == null) {
                return "\n";
            }
            GeneralFormatStyle style = Style.from(GeneralFormatStyle.class, documents);
            if (style != null) {
                return style.newLine();
            }
            return documents.printAll().indexOf('\r') >= 0 ? "\r\n" : "\n";
        }

        @Nullable
        private static Yaml parseClusterDefinitions(String yaml) {
            SourceFile parsed = new YamlParser().parse(yaml).findFirst().orElse(null);
            if (!(parsed instanceof Yaml.Documents documents) || documents.getDocuments().isEmpty()) {
                return null;
            }
            Yaml.Document document = documents.getDocuments().get(0);
            if (!(document.getBlock() instanceof Yaml.Mapping mapping)) {
                return null;
            }
            // as MergeYaml does, hand the document's own prefix to the first entry
            return mapping.withEntries(ListUtils.mapFirst(mapping.getEntries(), e -> e.withPrefix(document.getPrefix())));
        }

        @Nullable
        private static Yaml.Mapping.Entry entry(Yaml.Mapping mapping, String key) {
            return mapping.getEntries().stream()
                    .filter(e -> key.equals(keyValue(e)))
                    .findFirst()
                    .orElse(null);
        }

        @Nullable
        private static String keyValue(Yaml.Mapping.Entry entry) {
            return entry.getKey() instanceof Yaml.Scalar scalar ? scalar.getValue() : null;
        }

        @Nullable
        private static String scalarValue(@Nullable Yaml.Mapping.Entry entry) {
            return entry != null && entry.getValue() instanceof Yaml.Scalar scalar ? scalar.getValue() : null;
        }
    }

    /**
     * Reports whether any node of the visited subtree carries an anchor, or is an alias. Anchors are held as a field of
     * the node they decorate rather than being traversed as children, so they are checked in {@code preVisit}.
     */
    private static class AnchorOrAliasDetector extends YamlVisitor<AtomicBoolean> {

        @Override
        public Yaml preVisit(Yaml tree, AtomicBoolean found) {
            boolean anchored = (tree instanceof Yaml.Mapping mapping && mapping.getAnchor() != null)
                    || (tree instanceof Yaml.Sequence sequence && sequence.getAnchor() != null)
                    || (tree instanceof Yaml.Scalar scalar && scalar.getAnchor() != null)
                    || tree instanceof Yaml.Alias;
            if (anchored) {
                found.set(true);
            }
            return tree;
        }
    }
}
