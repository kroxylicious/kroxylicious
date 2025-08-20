/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.asciidoc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.asciidoctor.ast.Block;
import org.asciidoctor.ast.DescriptionList;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.ListItem;
import org.asciidoctor.ast.PhraseNode;
import org.asciidoctor.ast.Row;
import org.asciidoctor.ast.Section;
import org.asciidoctor.ast.Table;

class AdocNodeConverter {

    private static final String LINE_SEPARATOR = "\n";
    private static final String ATTR_ID = "id";
    private static final String ATTR_STYLE = "style";
    private static final String ATTR_LANG = "language";
    private static final String ATTR_HEADER = "header-option";
    private static final String ATTR_COLS = "cols";
    private static final String ATTR_FORMAT = "format";
    private static final String ATTR_CITETITLE = "citetitle";
    private static final String ATTR_ATTRIBUTION = "attribution";

    private final String mode;

    AdocNodeConverter(String engineName, String mode) {
        this.mode = mode;
    }

    public String convertDocumentNode(Document document) {
        StringBuilder result = new StringBuilder();
        if (isNotBlank(document.getTitle())) {
            String text = document.getTitle();
            result
                    .append("= ")
                    .append(text)
                    .append(LINE_SEPARATOR)
                    .append(LINE_SEPARATOR);
        }
        return result.append(document.getContent()).toString();
    }

    private boolean isNotBlank(String title) {
        return title != null && !title.isBlank();
    }

    // Sectionを以下のフォーマットで変換する.
    // === title
    //
    // content
    public String convertSectionNode(Section section) {
        String text = section.getTitle();
        return new StringBuilder()
                .append(String.join("", Collections.nCopies(section.getLevel() + 1, "=")))
                .append(" ")
                .append(text)
                .append(LINE_SEPARATOR)
                .append(LINE_SEPARATOR)
                .append(section.getContent())
                .toString();
    }

    // Blockを 以下のフォーマットで変換する.
    // attribute
    // .title
    // content
    public String convertParagraphNode(Block block) {
        StringBuilder result = new StringBuilder();
        // attribute
        result.append(getBlockNodeOption(block));
        // title
        result.append(getBlockNodeTitle(block));
        String text = block.getContent().toString();
        return result
                .append(text)
                .append(LINE_SEPARATOR)
                .toString();
    }

    // Blockを style: content のフォーマットで変換する.
    // ex. NOTE: Asciidoc is so good.
    public String convertAdmonitionNode(Block block) {
        StringBuilder result = new StringBuilder();
        // attribute
        result.append(getBlockNodeOption(block));
        // title
        result.append(getBlockNodeTitle(block));
        if (block.getAttribute(ATTR_STYLE) != null) {
            result.append(block.getAttribute(ATTR_STYLE).toString()).append(": ");
        }
        String text = block.getContent().toString();
        return result
                .append(text)
                .append(LINE_SEPARATOR)
                .toString();
    }

    // Blockを 半角スペース+content のフォーマットで変換する.
    // なお、元々のテキストが「行頭スペース」か「....」なのかは AST(Block)から判別不可.
    public String convertLiteralNode(Block block) {
        String text = block.getContent().toString();
        return new StringBuilder()
                .append(" ")
                .append(text)
                .append(LINE_SEPARATOR)
                .toString();
    }

    // Blockを以下のフォーマットで変換する.
    // .title
    // [style, attribution, citetitle]
    // ____
    // content
    // ____
    public String convertBlockQuote(Block block) {
        StringBuilder result = new StringBuilder();
        // title
        result.append(getBlockNodeTitle(block));
        // option
        result.append(getBlockQuoteOption(block));
        // content
        String text = block.getContent().toString();
        return result
                .append("____")
                .append(LINE_SEPARATOR)
                .append(text)
                .append(LINE_SEPARATOR)
                .append("____")
                .append(LINE_SEPARATOR)
                .toString();
    }

    // BlockQuoteのoption部分を抽出する.
    public String getBlockQuoteOption(Block block) {
        StringBuilder option = new StringBuilder();
        Map<String, Object> attr = block.getAttributes();
        if (attr != null && !attr.isEmpty()) {
            option.append("[");
            if (attr.get(ATTR_STYLE) != null) {
                option.append(attr.get(ATTR_STYLE).toString());
            }
            if (attr.get(ATTR_ATTRIBUTION) != null) {
                String text = attr.get(ATTR_ATTRIBUTION).toString();
                option.append(", ").append(text);
            }
            if (attr.get(ATTR_CITETITLE) != null) {
                String text = attr.get(ATTR_CITETITLE).toString();
                option.append(", ").append(text);
            }
            option.append("]").append(LINE_SEPARATOR);
        }
        return option.toString();
    }

    // Blockを以下のフォーマットで変換する.
    // [[id]]
    // [source,language]
    // .title
    // symbols
    // content（翻訳なし）
    // symbols
    public String convertBlockNodeContentBetweenSymbols(Block block, String symbols) {
        StringBuilder result = new StringBuilder();
        // attribute
        result.append(getBlockNodeOption(block));
        // title
        result.append(getBlockNodeTitle(block));
        // body
        return result
                .append(symbols)
                .append(LINE_SEPARATOR)
                .append(block.getContent())
                .append(LINE_SEPARATOR)
                .append(symbols)
                .append(LINE_SEPARATOR)
                .toString();
    }

    // Blockのtitle部分を抽出する.
    private String getBlockNodeTitle(Block block) {
        if (!isNotBlank(block.getTitle())) {
            return "";
        }
        String text = block.getTitle();
        return new StringBuilder()
                .append(".")
                .append(text)
                .append(LINE_SEPARATOR)
                .toString();
    }

    // BlockのOption部分を抽出する.
    // [[id]]
    // [source,language]
    // [%hardbreaks]
    private String getBlockNodeOption(Block block) {
        StringBuilder option = new StringBuilder();
        // ex. 1=source, 2=ruby, style=source, language=ruby, id=app-listing, title=app.rb
        Map<String, Object> attr = block.getAttributes();
        if (attr != null && !attr.isEmpty()) {
            // adocファイル内のリンクに使用されるID
            if (attr.get(ATTR_ID) != null) {
                option
                        .append("[[")
                        .append(attr.get(ATTR_ID).toString())
                        .append("]]")
                        .append(LINE_SEPARATOR);
            }
            // ソースブロックオプション
            if (attr.get(ATTR_STYLE) != null && attr.get(ATTR_STYLE).toString().equals("source")) {
                option.append("[").append(attr.get(ATTR_STYLE).toString());
                if (attr.get(ATTR_LANG) != null) {
                    option.append(",").append(attr.get(ATTR_LANG).toString());
                }
                option.append("]").append(LINE_SEPARATOR);
            }
            // 改行オプション
            if (attr.get("hardbreaks-option") != null) {
                option.append("[%hardbreaks]").append(LINE_SEPARATOR);
            }
        }
        return option.toString();
    }

    // Tableを以下のフォーマットで変換する.
    // .title
    // [options]
    // |===
    // header
    // body
    // |===
    public String convertTableNode(Table table) {
        // title
        StringBuilder title = new StringBuilder();
        if (isNotBlank(table.getTitle())) {
            String text = table.getTitle();
            title.append(".").append(text).append(LINE_SEPARATOR);
        }
        // option
        StringBuilder headerOption = new StringBuilder();
        if (table.getHeader() != null && !table.getHeader().isEmpty()) {
            java.util.List<StringBuilder> options = new ArrayList<>();
            // ヘッダー指定の有無
            if (table.getAttribute(ATTR_HEADER) != null) {
                options.add(new StringBuilder().append("options=\"header\""));
            }
            // カラム幅
            if (table.getAttribute(ATTR_COLS) != null) {
                options.add(
                        new StringBuilder()
                                .append("cols=\"")
                                .append(table.getAttribute(ATTR_COLS).toString())
                                .append("\""));
            }
            // フォーマット
            if (table.getAttribute(ATTR_FORMAT) != null) {
                options.add(
                        new StringBuilder()
                                .append("format=\"")
                                .append(table.getAttribute(ATTR_FORMAT).toString())
                                .append("\""));
            }
            if (!options.isEmpty()) {
                headerOption
                        .append("[")
                        .append(String.join(", ", options))
                        .append("]")
                        .append(LINE_SEPARATOR);
            }
        }

        // formatがCSVか判別
        boolean isCsvFormat = table.getAttribute(ATTR_FORMAT) != null
                && table.getAttribute(ATTR_FORMAT).toString().equals("csv");
        // Header
        StringBuilder header = new StringBuilder().append(convertRowList(table.getHeader(), isCsvFormat));
        // BODY
        StringBuilder body = new StringBuilder().append(convertRowList(table.getBody(), isCsvFormat));

        return title
                .append(headerOption)
                .append("|===")
                .append(LINE_SEPARATOR)
                .append(header)
                .append(body)
                .append("|===")
                .append(LINE_SEPARATOR)
                .toString();
    }

    // List<Row> を テーブルのカラム部分のフォーマットに変換する.
    private String convertRowList(java.util.List<Row> rows, boolean isCsvFormat) {
        StringBuilder result = new StringBuilder();
        for (Row row : rows) {
            if (isCsvFormat) {
                // CSV形式の場合はカンマでカラムを区切る.
                row.getCells().forEach(cell -> {
                    String text = cell.getText();
                    result.append(text).append(",");
                });
                // 末尾のカンマを削除した上で改行
                result.deleteCharAt(result.length() - 1).append(LINE_SEPARATOR);
            }
            else {
                // CSV形式以外の場合はパイプラインでカラムを区切る.
                row.getCells().forEach(cell -> {
                    String text = cell.getText();
                    result.append("|").append(text);
                });
                result.append(LINE_SEPARATOR);
            }
        }
        return result.toString();
    }

    // Listを marker text のフォーマットで変換する.
    // ex.
    // .title
    // * level1
    // ** level2
    // *** level3
    public String convertlistNode(org.asciidoctor.ast.List list) {
        StringBuilder result = new StringBuilder();
        // ListTitle
        if (isNotBlank(list.getTitle())) {
            String text = list.getTitle();
            result.append(".").append(text).append(LINE_SEPARATOR);
        }
        // ListItem
        list.findBy(Map.of("context", ":list_item"))
                .forEach(item -> result.append(convertListItem((ListItem) item)));
        return result.toString();
    }

    // DescriptionListを marker text のフォーマットで変換する.
    // ex.
    // . level1
    // .. level2
    // ... level3
    public String convertDlistNode(DescriptionList dlist) {
        StringBuilder result = new StringBuilder();
        // ListTitle
        if (isNotBlank(dlist.getTitle())) {
            String text = dlist.getTitle();
            result.append(".").append(text).append(LINE_SEPARATOR);
        }
        // DListItems
        dlist.getItems().forEach(item -> {
            var terms = item.getTerms().stream()
                    .filter(Objects::nonNull)
                    .map(this::convertDListTerm)
                    .collect(Collectors.joining(","));
            result.append(terms);
            result.append(":: ");
            result.append(convertListItem(item.getDescription()));
        });
        return result.append(LINE_SEPARATOR).toString();
    }

    public String convertDListTerm(ListItem term) {
        if (term == null || !term.hasText()) {
            return "";
        }
        return term.getText();
    }

    // ListItemを marker text のフォーマットで変換する.
    // ex. *** section3
    public String convertListItem(ListItem listItem) {
        if (listItem == null || !listItem.hasText()) {
            return "";
        }

        var stringBuilder = new StringBuilder();
        if (listItem.getMarker() != null) {
            stringBuilder.append(listItem.getMarker())
                    .append(" ");
        }
        stringBuilder.append(listItem.getText())
                .append(LINE_SEPARATOR);
        return stringBuilder.toString();
    }

    // PhraseNodeを [[id, text]] のフォーマットで変換する.
    public String convertInlineAnchor(PhraseNode phrase) {
        StringBuilder result = new StringBuilder();
        result.append("[[").append(phrase.getId());
        if (isNotBlank(phrase.getText())) {
            result.append(", ").append(phrase.getText());
        }
        result.append("]]");
        return result.toString();
    }

    public String convertInlineCallout(PhraseNode phrase) {
        String text = phrase.getText();

        // AsciiDoc API seemingly doesn't let us recover the comment character that proceeded the callout.
        var parent = phrase.getParent();
        var commentChar = Optional.ofNullable(parent.getAttribute("line-comment")).map(String::valueOf);

        if (commentChar.isEmpty()) {
            commentChar = Optional.ofNullable(parent.getAttribute("language")).map(String::valueOf).map(AdocNodeConverter::langToCommentChar);
        }

        var stringBuilder = new StringBuilder();
        commentChar.ifPresent(cc -> stringBuilder.append(cc).append(" "));
        stringBuilder.append("<")
                .append(text)
                .append(">");
        return stringBuilder.toString();
    }

    private static String langToCommentChar(String languageName) {
        if (Set.of("yaml", "shell").contains(languageName)) {
            return "#";
        }
        else if (languageName.equals("java")) {
            return "//";
        }
        throw new IllegalArgumentException("Don't know the comment convention for " + languageName);
    }

    // PhraseNodeを text + のフォーマットで変換する.
    public String convertInlineBreakNode(PhraseNode phrase) {
        String text = phrase.getText();
        return new StringBuilder().append(text).append(" +").toString();
    }

}