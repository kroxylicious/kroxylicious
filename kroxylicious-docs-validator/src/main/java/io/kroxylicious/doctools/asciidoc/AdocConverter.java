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
import org.asciidoctor.ast.PhraseNode;
import org.asciidoctor.ast.Section;
import org.asciidoctor.ast.Table;
import org.asciidoctor.converter.ConverterFor;
import org.asciidoctor.converter.StringConverter;

/**
 * An AsciiDoc Converter that writes back to AsciiDoc.
 *
 * Based on AdocConverter from <a href="https://github.com/project-au-lait/batch-translator">https://github.com/project-au-lait/batch-translator</a>.
 */
@ConverterFor("adoc")
public class AdocConverter extends StringConverter {

    private final AdocNodeConverter adocNodeConverter;

    public AdocConverter(String backend, Map<String, Object> opts) {
        super(backend, opts);
        // DocumentNodeから翻訳エンジンの名称を取得し、adocNodeConverterを生成する.
        Document document = (Document) opts.get("document");
        this.adocNodeConverter = new AdocNodeConverter(
                String.valueOf(document.getAttribute("engine")),
                String.valueOf(document.getAttribute("mode")));
    }

    @Override
    public String convert(ContentNode node, String transform, Map<Object, Object> o) {
        if (transform == null) {
            transform = node.getNodeName();
        }

        if (node instanceof Document) {
            return adocNodeConverter.convertDocumentNode((Document) node);
        }
        else if (node instanceof Section) {
            return adocNodeConverter.convertSectionNode((Section) node);
        }
        else if (node instanceof Block) {
            // Block要素は複数パターンが存在するので場合分けして処理する
            // open, listing, pass要素内 の テキスト（paragraph）は翻訳しない
            return switch (transform) {
                case "paragraph" -> {
                    if (node.getParent() != null
                            && node.getParent().getNodeName().matches("open|listing|pass")) {
                        yield ((Block) node).getContent().toString();
                    }
                    yield adocNodeConverter.convertParagraphNode((Block) node);
                }
                case "admonition" -> adocNodeConverter.convertAdmonitionNode((Block) node);
                case "literal" -> adocNodeConverter.convertLiteralNode((Block) node);
                case "quote" -> adocNodeConverter.convertBlockQuote((Block) node);
                case "open" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols((Block) node, "--");
                case "listing" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols((Block) node, "----");
                case "pass" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols((Block) node, "++++");
                case "sidebar" -> adocNodeConverter.convertBlockNodeContentBetweenSymbols((Block) node, "****");
                case "image", "floating_title" -> ""; // TODO
                case "thematic_break" ->
                        // 水平線を出力
                        new StringBuilder().append("---").append("\n").toString();
                default -> convertOther(transform, (Block) node);
            };
        }
        else if (node instanceof Table) {
            return adocNodeConverter.convertTableNode((Table) node);
        }
        else if (node instanceof org.asciidoctor.ast.List) {
            return adocNodeConverter.convertlistNode((org.asciidoctor.ast.List) node);
        }
        else if (node instanceof DescriptionList) {
            return adocNodeConverter.convertDlistNode((DescriptionList) node);
        }
        else if (node instanceof PhraseNode) {
            if (transform.equals("inline_anchor")) {
                return adocNodeConverter.convertInlineAnchor((PhraseNode) node);
            }
            else if (transform.equals("inline_callout")) {
                return adocNodeConverter.convertInlineCallout((PhraseNode) node);
            }
            else {
                return adocNodeConverter.convertInlineBreakNode((PhraseNode) node);
            }
        }
        return "";
    }

    private static String convertOther(String transform, Block node) {
        if (node.getContent() == null) {
            throw new IllegalArgumentException("No content for transform " + transform);
        }
        return node.getContent().toString();
    }
}