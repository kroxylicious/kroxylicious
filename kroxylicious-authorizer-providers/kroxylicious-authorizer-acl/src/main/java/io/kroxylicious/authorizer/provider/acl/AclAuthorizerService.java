/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import io.kroxylicious.authorizer.provider.acl.parser.AclRulesBaseListener;
import io.kroxylicious.authorizer.provider.acl.parser.AclRulesLexer;
import io.kroxylicious.authorizer.provider.acl.parser.AclRulesParser;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = AclAuthorizerConfig.class)
public class AclAuthorizerService implements AuthorizerService<AclAuthorizerConfig> {

    private @Nullable AclAuthorizer aclAuthorizer;

    @Override
    public void initialize(@Nullable AclAuthorizerConfig config1) {
        var config = Plugins.requireConfig(this, config1);
        var fileName = config.aclFile();
        try {
            var stream = CharStreams.fromPath(Path.of(fileName));
            this.aclAuthorizer = parse(stream);
        }
        catch (java.io.IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Authorizer build() throws IllegalStateException {
        return Objects.requireNonNull(aclAuthorizer);
    }

    interface ErrorCollector {

        int numErrors();

        int maxErrorsToReport();

        List<String> errorMessages();

        default void maybeThrow(String message) {
            if (this.numErrors() > 0) {
                StringBuilder msg = new StringBuilder();
                msg.append(message.formatted(this.numErrors()));
                if (this.numErrors() > this.maxErrorsToReport()) {
                    msg.append(" (showing first %d)".formatted(this.maxErrorsToReport()));
                }
                msg.append(System.lineSeparator());
                this.errorMessages().forEach(e -> msg.append(e).append(System.lineSeparator()));
                throw new InvalidRulesFileException(msg.toString(), this.errorMessages());
            }
        }
    }

    static <O extends Enum<O> & ResourceType<O>> AclAuthorizer parse(CharStream stream) {
        int maxErrorsToReports = 100;
        ParseErrorListener parseErrorListener = new ParseErrorListener(maxErrorsToReports);

        var lexer = new AclRulesLexer(stream);
        lexer.addErrorListener(parseErrorListener);
        var tokenStream = new CommonTokenStream(lexer);
        var parser = new AclRulesParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(parseErrorListener);
        var tree = parser.rule_();
        parseErrorListener.maybeThrow();
        var builder = AclAuthorizer.builder();

        ParseTreeWalker walker = new ParseTreeWalker();
        BuildingListener buildingListener = new BuildingListener(builder, maxErrorsToReports);
        walker.walk(buildingListener, tree);
        buildingListener.maybeThrow();

        return builder.build();
    }

    static class ParseErrorListener extends BaseErrorListener implements ErrorCollector {
        private final List<String> errorMessages;
        private final int maxErrorsToReport;
        private int numErrors = 0;

        ParseErrorListener(int maxErrorsToReport) {
            this.maxErrorsToReport = maxErrorsToReport;
            this.errorMessages = new ArrayList<>(maxErrorsToReport);
        }

        @Override
        public List<String> errorMessages() {
            return errorMessages;
        }

        @Override
        public int numErrors() {
            return numErrors;
        }

        @Override
        public int maxErrorsToReport() {
            return maxErrorsToReport;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line,
                                int charPositionInLine,
                                String msg,
                                RecognitionException e) {
            numErrors++;
            if (errorMessages.size() < maxErrorsToReport) {
                errorMessages.add("%d:%d: %s".formatted(line, charPositionInLine, msg.endsWith(".") ? msg : msg + "."));
            }
        }

        public void maybeThrow() {
            maybeThrow("Found %d syntax errors");
        }
    }

    private static class BuildingListener extends AclRulesBaseListener implements ErrorCollector {

        private final AclAuthorizer.Builder builder;
        private final int maxErrorsToReport;
        private final List<String> errorMessages;
        private int numErrors;
        private boolean allOps;
        Map<String, String> localToQualified = new HashMap<>();
        private @Nullable AclAuthorizer.SubjectSelectorBuilder subjectBuilder;
        private @Nullable AclAuthorizer.PrincipalSelectorBuilder principalBuilder;
        private @Nullable Set<String> opNames;
        private @Nullable AclAuthorizer.OperationsBuilder operationsBuilder;
        private @Nullable AclAuthorizer.ResourceBuilder<? extends Enum> resourceBuilder;

        BuildingListener(AclAuthorizer.Builder builder, int maxErrorsToReport) {
            this.builder = builder;
            this.maxErrorsToReport = maxErrorsToReport;
            this.errorMessages = new ArrayList<>(maxErrorsToReport);
        }

        @Override
        public List<String> errorMessages() {
            return errorMessages;
        }

        @Override
        public int numErrors() {
            return numErrors;
        }

        @Override
        public int maxErrorsToReport() {
            return maxErrorsToReport;
        }

        void reportError(Token token, String error) {
            numErrors++;
            if (errorMessages.size() < maxErrorsToReport) {
                errorMessages.add("%d:%d: %s".formatted(token.getLine(), token.getCharPositionInLine(), error));
            }
        }

        /**
         * Removes double quote or forward slash delimiters from literals.
         * @param string The node to remove the delimiters from.
         * @return The un-delimited string
         */
        private static String unwrapDelims(TerminalNode string) {
            var text = string.getText();
            return text.substring(1, text.length() - 1);
        }

        private static String unquote(String sansQuotes) {
            return sansQuotes.replaceAll("\\\\(.)", "$1");
        }

        private String unquoteString(TerminalNode string) {
            return unquote(unwrapDelims(string));
        }

        private String unquoteRegex(TerminalNode string) {
            return unquote(unwrapDelims(string));
        }

        public void maybeThrow() {
            maybeThrow("Found %d errors");
        }

        record Prefix(String prefix, boolean eq) {
            public boolean isEmpty() {
                return prefix.isEmpty();
            }
        }

        private Prefix unquotePrefix(AclRulesParser.NameLikeContext nameLike) {
            var pattern = unwrapDelims(nameLike.STRING());
            boolean wildcardIsPresent = false;
            var prefix = new StringBuilder(pattern.length());
            for (int i = 0; i < pattern.length(); i++) {
                char ch = pattern.charAt(i);
                if (ch == '\\' && i + 1 < pattern.length()) {
                    char ch2 = pattern.charAt(++i);
                    prefix.append(ch2);
                }
                else if (ch == '*') {
                    wildcardIsPresent = true;
                    if (i != pattern.length() - 1) {
                        reportError(nameLike.STRING().getSymbol(), "Wildcard '*' only supported as last character in 'like'.");
                    }
                }
                else {
                    prefix.append(ch);
                }
            }
            return new Prefix(prefix.toString(), !wildcardIsPresent);
        }

        @Override
        public void enterVersionStmt(AclRulesParser.VersionStmtContext ctx) {
            var version = Integer.parseInt(ctx.INT().getText());
            if (version != 1) {
                reportError(ctx.start, "Unsupported version: Only version 1 is supported.");
            }
        }

        @Override
        public void enterImportStmt(AclRulesParser.ImportStmtContext ctx) {
            var packageName = ctx.packageName().qualIdent().ident().stream()
                    .map(RuleContext::getText)
                    .collect(Collectors.joining("."));
            String simpleClassName = ctx.name.getText();
            String localName;
            Token errorToken;
            if (ctx.local != null) {
                localName = ctx.local.getText();
                errorToken = ctx.local.start;
            }
            else {
                localName = simpleClassName;
                errorToken = ctx.name.start;
            }
            var was = this.localToQualified.put(localName, packageName + "." + simpleClassName);
            if (was != null) {
                reportError(errorToken,
                        "Local name '%s' is already being used for class %s.".formatted(localName, was));
            }
        }

        @Override
        public void enterAllowRule(AclRulesParser.AllowRuleContext ctx) {
            this.subjectBuilder = this.builder.allow();
        }

        @Override
        public void enterDenyRule(AclRulesParser.DenyRuleContext ctx) {
            this.subjectBuilder = this.builder.deny();
        }

        @Override
        public void enterPrincipalType(AclRulesParser.PrincipalTypeContext ctx) {
            var principalClass = lookupClass(ctx.ident().getText(), ctx.ident().start, Principal.class, "Principal");
            if (principalClass != null) {
                this.principalBuilder = this.subjectBuilder.subjectsHavingPrincipal(principalClass);
            }
            this.subjectBuilder = null;
        }

        private <T> @Nullable Class<? extends T> lookupClass(String ident, Token identToken, Class<T> cls, String desc) {
            // look it up in the imports
            String localClassName = ident;
            var qualifiedClassName = this.localToQualified.get(localClassName);
            if (qualifiedClassName == null) {
                reportError(identToken,
                        "%s class with name '%s' has not been imported.".formatted(desc, localClassName));
                return null;
            }
            try {
                Class<?> c = Class.forName(qualifiedClassName);
                if (!cls.isAssignableFrom(c)) {
                    reportError(identToken,
                            "%s class '%s' is not a subclass of %s.".formatted(desc, localClassName, cls));
                    return null;
                }
                return c.asSubclass(cls);
            }
            catch (ClassNotFoundException e) {
                reportError(identToken,
                        "%s class '%s' was not found.".formatted(desc, qualifiedClassName));
                return null;
            }
        }

        @Override
        public void enterNameEq(AclRulesParser.NameEqContext ctx) {
            if (this.resourceBuilder != null) {
                this.resourceBuilder.onResourceWithNameEqualTo(unquoteString(ctx.STRING()));
                this.resourceBuilder = null;
            }
            else if (this.principalBuilder != null) {
                this.operationsBuilder = this.principalBuilder.withNameEqualTo(unquoteString(ctx.STRING()));
                this.principalBuilder = null;
            }
        }

        @Override
        public void enterNameIn(AclRulesParser.NameInContext nameIn) {
            var names = nameIn.STRING().stream()
                    .map(this::unquoteString)
                    .collect(Collectors.toSet());
            if (this.resourceBuilder != null) {
                this.resourceBuilder.onResourcesWithNameIn(names);
                this.resourceBuilder = null;
            }
            else if (this.principalBuilder != null) {
                this.operationsBuilder = this.principalBuilder.withNameIn(names);
                this.principalBuilder = null;
            }
        }

        @Override
        public void enterNameLike(AclRulesParser.NameLikeContext nameLike) {
            Prefix prefix1 = unquotePrefix(nameLike);
            if (this.resourceBuilder != null) {
                if (prefix1.eq()) {
                    this.resourceBuilder.onResourceWithNameEqualTo(prefix1.prefix());
                }
                else if (prefix1.isEmpty()) {
                    this.resourceBuilder.onAllResources();
                }
                else {
                    this.resourceBuilder.onResourcesWithNameStartingWith(prefix1.prefix());
                }
                this.resourceBuilder = null;
            }
            else if (this.principalBuilder != null) {
                if (prefix1.eq()) {
                    this.operationsBuilder = this.principalBuilder.withNameEqualTo(prefix1.prefix());
                }
                else if (prefix1.isEmpty()) {
                    this.operationsBuilder = this.principalBuilder.withAnyName();
                }
                else {
                    this.operationsBuilder = this.principalBuilder.withNameStartingWith(prefix1.prefix());
                }
                this.principalBuilder = null;
            }
        }

        @Override
        public void enterNameMatch(AclRulesParser.NameMatchContext nameMatch) {
            if (this.resourceBuilder == null) {
                reportError(nameMatch.MATCHING().getSymbol(),
                        "'%s' operation not supported on principals".formatted(nameMatch.MATCHING().getText()));
            }
            String p = unquoteRegex(nameMatch.REGEX());
            try {
                Pattern.compile(p); // check it's valid
                if (this.resourceBuilder != null) {
                    this.resourceBuilder.onResourcesWithNameMatching(p);
                }
            }
            catch (PatternSyntaxException e) {
                reportError(nameMatch.REGEX().getSymbol(),
                        "Regex provided for '%s' operation is not valid: %s.".formatted(
                                nameMatch.MATCHING().getText(),
                                e.getMessage()));
            }
            this.resourceBuilder = null;
        }

        @Override
        public void enterNameAny(AclRulesParser.NameAnyContext ctx) {
            if (this.resourceBuilder != null) {
                this.resourceBuilder.onAllResources();
                this.resourceBuilder = null;
            }
            else if (this.principalBuilder != null) {
                this.operationsBuilder = this.principalBuilder.withAnyName();
                this.principalBuilder = null;
            }
        }

        @Override
        public void enterOperations(AclRulesParser.OperationsContext ctx) {
            if (ctx.STAR() != null) {
                this.allOps = true;
                this.opNames = null;
            }
            else if (ctx.operation() != null) {
                this.allOps = false;
                this.opNames = Set.of(ctx.operation().ident().getText());
            }
            else if (ctx.operationSet() != null) {
                this.allOps = false;
                this.opNames = ctx.operationSet().operation().stream()
                        .map(AclRulesParser.OperationContext::ident)
                        .map(ParseTree::getText)
                        .collect(Collectors.toSet());
            }
            else {
                throw new IllegalStateException();
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public void enterResource(AclRulesParser.ResourceContext ctx) {
            var cls = lookupClass(ctx.ident().getText(), ctx.ident().start, ResourceType.class, "ResourceType");
            if (cls != null && this.operationsBuilder != null) {
                var enumCls = cls.asSubclass(Enum.class);
                if (allOps) {
                    this.resourceBuilder = this.operationsBuilder.allOperations((Class) enumCls);
                }
                else {
                    List<? extends Enum> list = Objects.requireNonNull(opNames).stream()
                            .map(name -> Enum.valueOf(enumCls, name))
                            .toList();
                    this.resourceBuilder = this.operationsBuilder.operations(EnumSet.copyOf(list));
                }
            }

            this.operationsBuilder = null;
            this.opNames = null;
            this.allOps = false;
        }
    }
}
