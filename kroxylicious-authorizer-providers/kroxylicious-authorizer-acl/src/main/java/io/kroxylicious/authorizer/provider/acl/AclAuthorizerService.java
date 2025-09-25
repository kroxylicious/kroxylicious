/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.kroxylicious.authorizer.provider.acl.parser.RuleBaseListener;
import io.kroxylicious.authorizer.provider.acl.parser.RuleLexer;
import io.kroxylicious.authorizer.provider.acl.parser.RuleParser;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.authorizer.service.Operation;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = AclAuthorizerConfig.class)
public class AclAuthorizerService implements AuthorizerService<AclAuthorizerConfig> {

    private AclAuthorizerConfig config;

    @Override
    public void initialize(AclAuthorizerConfig config) {
        this.config = config;
    }

    @NonNull
    @Override
    public Authorizer build() throws IllegalStateException {
        var fileName = config.aclFile();
        try {
            var stream = CharStreams.fromPath(Path.of(fileName));
            return parse(stream);
        }
        catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    static <O extends Enum<O> & Operation<O>> AclAuthorizer parse(CharStream stream) {
        ParseErrorListener listener = new ParseErrorListener(100);
        var lexer = new RuleLexer(stream);
        lexer.addErrorListener(listener);
        var tokenStream = new CommonTokenStream(lexer);
        var parser = new RuleParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(listener);
        var tree = parser.rule_();
        listener.maybeThrow();
        var builder = AclAuthorizer.builder();
        ParseTreeWalker walker = new ParseTreeWalker();
        List<String> errors = new ArrayList<>();
        walker.walk(new BuildingListener(builder, errors), tree);
        if (!errors.isEmpty()) {
            throw new InvalidRulesFileException("Found %d errors".formatted(errors.size()), errors);
        }
        return builder.build();
    }

    static class ParseErrorListener extends BaseErrorListener {
        private int numErrors = 0;
        private List<String> errorMessages = new ArrayList<>();
        private final int maxErrorsToReport;

        ParseErrorListener(int maxErrorsToReport) {
            this.maxErrorsToReport = maxErrorsToReport;
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
            if (numErrors > 0) {
                throw new InvalidRulesFileException("Found %d syntax errors".formatted(numErrors), errorMessages);
            }
        }
    }

    private static class BuildingListener extends RuleBaseListener {

        private final AclAuthorizer.Builder builder;
        private final List<String> errors;
        private boolean allOps;
        Map<String, String> localToQualified = new HashMap<>();
        private AclAuthorizer.SubjectSelectorBuilder subjectBuilder;
        private AclAuthorizer.PrincipalSelectorBuilder principalBuilder;
        private Set<String> opNames;
        private AclAuthorizer.OperationsBuilder operationsBuilder;
        private AclAuthorizer.ResourceBuilder<? extends Enum> resourceBuilder;

        BuildingListener(AclAuthorizer.Builder builder, List<String> errors) {
            this.builder = builder;
            this.errors = errors;
        }

        void reportError(
                Token token, String error) {
            errors.add("%d:%d: %s".formatted(token.getLine(), token.getCharPositionInLine(), error));
        }

        String unwrap(TerminalNode string) {
            var text = string.getText();
            // TODO needs to handle unquoting
            return text.substring(1, text.length() -1);
        }

        @Override
        public void enterImportStmt(RuleParser.ImportStmtContext ctx) {
            var packageName = ctx.packageName().qualIdent().IDENT().stream()
                    .map(TerminalNode::getText)
                    .collect(Collectors.joining("."));
            String simpleClassName = ctx.name.getText();
            String localName;
            if (ctx.local != null) {
                localName = ctx.local.getText();
            }
            else {
                localName = simpleClassName;
            }
            var was = localToQualified.put(localName, packageName + "." + simpleClassName);
            if (was != null) {
                reportError(ctx.name,
                        "Local name '%s' is already being used for class %s".formatted(localName, was));
            }
        }

        @Override
        public void enterAllowRule(RuleParser.AllowRuleContext ctx) {
            this.subjectBuilder = builder.grant();
        }

        @Override
        public void enterPrincipalType(RuleParser.PrincipalTypeContext ctx) {
            var principalClass = lookupClass(ctx.IDENT(), Principal.class, "Principal");
            if (principalClass != null) {
                this.principalBuilder = subjectBuilder.subjectsHavingPrincipal(principalClass);
            }
            subjectBuilder = null;
        }

        @NonNull
        private <T> @Nullable Class<? extends T> lookupClass(TerminalNode node, Class<T> cls, String desc) {
            // look it up in the imports
            String localClassName = node.getText();
            var qualifiedClassName = localToQualified.get(localClassName);
            if (qualifiedClassName == null) {
                reportError(node.getSymbol(),
                        "%s class with name '%s' has not been imported.".formatted(desc, localClassName));
                return null;
            }
            try {
                Class<?> c = Class.forName(qualifiedClassName);
                if (!cls.isAssignableFrom(c)) {
                    reportError(node.getSymbol(),
                            "%s class '%s' is not a subclass of %s.".formatted(desc, localClassName, cls));
                    return null;
                }
                return c.asSubclass(cls);
            }
            catch (ClassNotFoundException e) {
                reportError(node.getSymbol(),
                        "%s class '%s' was not found.".formatted(desc, qualifiedClassName));
                return null;
            }
        }

        @Override
        public void enterEqOp(RuleParser.EqOpContext ctx) {
            if (resourceBuilder != null) {
                resourceBuilder.onResourceWithNameEqualTo(unwrap(ctx.nameEq().STRING()));
                resourceBuilder = null;
            }
            else if (principalBuilder != null) {
                this.operationsBuilder = principalBuilder.withNameEqualTo(unwrap(ctx.nameEq().STRING()));
                this.principalBuilder = null;
            }
        }

        @Override
        public void enterNameIn(RuleParser.NameInContext nameIn) {
            if (resourceBuilder == null) {
                reportError(nameIn.IN().getSymbol(),
                        "'%s' operation not supported on principals".formatted(nameIn.IN().getText()));
            }
            var names = nameIn.STRING().stream()
                    .map(this::unwrap)
                    .collect(Collectors.toSet());
            resourceBuilder.onResourcesWithNameIn(names);
            resourceBuilder = null;
        }

        @Override
        public void enterNameLike(RuleParser.NameLikeContext nameLike) {
            if (resourceBuilder == null) {
                reportError(nameLike.LIKE().getSymbol(),
                        "'%s' operation not supported on principals".formatted(nameLike.LIKE().getText()));
            }
            var pattern = unwrap(nameLike.STRING());
            int firstStartIndex = pattern.indexOf('*');
            if (firstStartIndex == -1) {
                resourceBuilder.onResourceWithNameEqualTo(pattern);
                resourceBuilder = null;
            }
            else if (firstStartIndex != pattern.length() -1) {
                reportError(nameLike.STRING().getSymbol(), "Wildcard '*' only supported as last character in 'like'");
            }
            else {
                resourceBuilder.onResourcesWithNameStartingWith(pattern.substring(0, firstStartIndex));
                resourceBuilder = null;
            }
        }

        @Override
        public void enterNameMatch(RuleParser.NameMatchContext nameMatch) {
            if (resourceBuilder == null) {
                reportError(nameMatch.MATCHING().getSymbol(),
                        "'%s' operation not supported on principals".formatted(nameMatch.MATCHING().getText()));
            }
            String p = unwrap(nameMatch.REGEX());
            try {
                Pattern.compile(p); // check it's valid
            }
            catch (PatternSyntaxException e) {
                reportError(nameMatch.REGEX().getSymbol(),
                        "Regex provided for '%s' operation is not valid: %s".formatted(
                                nameMatch.MATCHING().getText(),
                                e.getMessage()));
            }
            if (resourceBuilder != null) {
                resourceBuilder.onResourcesWithNameMatching(p);
                resourceBuilder = null;
            }
        }

        @Override
        public void enterWildcardOp(RuleParser.WildcardOpContext ctx) {
            if (resourceBuilder == null) {
                reportError(ctx.STAR().getSymbol(),
                        "Wildcard operation '%s' not supported on principals".formatted(ctx.STAR().getText()));
            }
            resourceBuilder.onAllResources();
            resourceBuilder = null;
        }

        @Override
        public void enterOperations(RuleParser.OperationsContext ctx) {
            if (ctx.STAR() != null) {
                this.allOps = true;
                this.opNames = null;
            }
            else if (ctx.operation() != null) {
                this.allOps = false;
                this.opNames = Set.of(ctx.operation().IDENT().getText());
            }
            else if (ctx.operationSet() != null) {
                this.allOps = false;
                this.opNames = ctx.operationSet().operation().stream().map(RuleParser.OperationContext::IDENT)
                        .map(TerminalNode::getText)
                        .collect(Collectors.toSet());
            }
            else {
                throw new IllegalStateException();
            }
        }

        @Override
        public void enterResource(RuleParser.ResourceContext ctx) {
            var cls = lookupClass(ctx.IDENT(), Operation.class, "Operation");
            if (cls != null) {
                if (operationsBuilder != null) {
                    var enumCls = cls.asSubclass(Enum.class);
                    if (allOps) {
                        this.resourceBuilder = operationsBuilder.allOperations((Class) enumCls);
                    }
                    else {
                        var list = (List) opNames.stream().map(name -> Enum.valueOf(enumCls, name)).toList();
                        this.resourceBuilder = operationsBuilder.operations(EnumSet.copyOf(list));
                    }
                }
            }
            this.operationsBuilder = null;
            this.opNames = null;
            this.allOps = false;
        }
    }
}
