/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.asciidoc;

import java.util.Map;

import org.asciidoctor.ast.Block;
import org.asciidoctor.ast.ContentNode;
import org.asciidoctor.ast.DescriptionList;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.List;
import org.asciidoctor.ast.PhraseNode;
import org.asciidoctor.ast.Section;
import org.asciidoctor.ast.Table;
import org.asciidoctor.converter.ConverterFor;
import org.asciidoctor.converter.StringConverter;

/**
 * An AsciiDoc Converter that writes back to AsciiDoc.
 * <br/>
 * Based on AdocConverter from <a href="https://github.com/project-au-lait/batch-translator">https://github.com/project-au-lait/batch-translator</a>.
 */
@ConverterFor("adoc")
public class AdocConverter extends StringConverter {

    private final AdocNodeConverter adocNodeConverter = new AdocNodeConverter();

    public AdocConverter(String backend, Map<String, Object> opts) {
        super(backend, opts);
    }

    @Override
    public String convert(ContentNode node, String transform, Map<Object, Object> o) {
        if (transform == null) {
            transform = node.getNodeName();
        }

        return switch (node) {
            case Document document -> adocNodeConverter.convertDocumentNode(document);
            case Section section -> adocNodeConverter.convertSectionNode(section);
            // Block要素は複数パターンが存在するので場合分けして処理する
            // open, listing, pass要素内 の テキスト（paragraph）は翻訳しない
            case Block block -> switch (transform) {
                case "paragraph" -> {
                    if (node.getParent() != null
                            && node.getParent().getNodeName().matches("open|listing|pass")) {
                        yield block.getContent().toString();
                    }
                    yield adocNodeConverter.convertParagraphNode(block);
                }
                case "admonition" -> adocNodeConverter.convertAdmonitionNode(block);
                case "literal" -> adocNodeConverter.convertLiteralNode(block);
                case "quote" -> adocNodeConverter.convertBlockQuote(block);
                case "open" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols(block, "--");
                case "listing" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols(block, "----");
                case "pass" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols(block, "++++");
                case "sidebar" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols(block, "****");
                case "image" -> adocNodeConverter.convertImageNode(block);
                case "floating_title" -> adocNodeConverter.convertFloatingTitle(block);
                case "thematic_break" ->
                        // 水平線を出力
                        "---\n";
                default -> adocNodeConverter.convertOther(transform, block);
            };
            case Table table -> adocNodeConverter.convertTableNode(table);
            case List list -> adocNodeConverter.convertlistNode(list);
            case DescriptionList descriptionList -> adocNodeConverter.convertDlistNode(descriptionList);
            case PhraseNode phraseNode -> switch (transform) {
                case "inline_anchor" -> adocNodeConverter.convertInlineAnchor(phraseNode);
                case "inline_callout" -> adocNodeConverter.convertInlineCallout(phraseNode);
                default -> adocNodeConverter.convertInlineBreakNode(phraseNode);
            };
            default -> throw new IllegalStateException("Unknown node type (%s)  or transform (%s)".formatted(node.getClass().getName(), transform));
        };
    }

}