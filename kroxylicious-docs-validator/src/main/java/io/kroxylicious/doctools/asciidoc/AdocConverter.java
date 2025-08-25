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

        if (node instanceof Document document) {
            return adocNodeConverter.convertDocumentNode(document);
        }
        else if (node instanceof Section section) {
            return adocNodeConverter.convertSectionNode(section);
        }
        else if (node instanceof Block block) {
            // Block要素は複数パターンが存在するので場合分けして処理する
            // open, listing, pass要素内 の テキスト（paragraph）は翻訳しない
            return switch (transform) {
                case "paragraph" -> {
                    if (node.getParent() != null
                            && node.getParent().getNodeName().matches("open|listing|pass")) {
                        yield (block).getContent().toString();
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
                        new StringBuilder().append("---").append("\n").toString();
                default -> adocNodeConverter.convertOther(transform, block);
            };
        }
        else if (node instanceof Table table) {
            return adocNodeConverter.convertTableNode(table);
        }
        else if (node instanceof List list) {
            return adocNodeConverter.convertlistNode(list);
        }
        else if (node instanceof DescriptionList descriptionList) {
            return adocNodeConverter.convertDlistNode(descriptionList);
        }
        else if (node instanceof PhraseNode phraseNode) {
            if (transform.equals("inline_anchor")) {
                return adocNodeConverter.convertInlineAnchor(phraseNode);
            }
            else if (transform.equals("inline_callout")) {
                return adocNodeConverter.convertInlineCallout(phraseNode);
            }
            else {
                return adocNodeConverter.convertInlineBreakNode(phraseNode);
            }
        }
        return "";
    }

}