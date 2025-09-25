/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
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
        return parseFile(fileName);
    }

    @NonNull
    private static <O extends Enum<O> & Operation<O>> AclAuthorizer parseFile(String fileName) {
        try {
            var stream = CharStreams.fromPath(Path.of(fileName));
            var lexer = new RuleLexer(stream);
            var tokenStream = new CommonTokenStream(lexer);
            var parser = new RuleParser(tokenStream);
            var tree = parser.rule_();
            var builder = AclAuthorizer.builder();
            ParseTreeWalker walker = new ParseTreeWalker();
            walker.walk(new BuildingListener(builder), tree);
            return builder.build();
        }
        catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class BuildingListener extends RuleBaseListener {

        private final AclAuthorizer.Builder builder;
        private boolean allOps;
        Map<String, String> localToQualified;
        private AclAuthorizer.SubjectSelectorBuilder subjectBuilder;
        private AclAuthorizer.PrincipalSelectorBuilder principalBuilder;
        private Set<String> opNames;
        private AclAuthorizer.OperationsBuilder operationsBuilder;
        private AclAuthorizer.ResourceBuilder<? extends Enum> resourceBuilder;

        BuildingListener(AclAuthorizer.Builder builder) {
            this.builder = builder;
        }

        void reportError(
                Token token, String error) {
            throw new IllegalArgumentException("%d:%d: %s".formatted(token.getLine(), token.getCharPositionInLine(), error));
        }

        String unwrap(TerminalNode string) {
            var text = string.getText();
            // TODO needs to handle unquoting
            return text.substring(1, text.length() -1);
        }

        @Override
        public void enterImportStmt(RuleParser.ImportStmtContext ctx) {
            var packageName = ctx.packageName().qualIdent().DOT().stream()
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
                reportError(ctx.name, "Local name %s is already being used for class %s".formatted(localName, was));
            }
        }

        @Override
        public void enterAllowRule(RuleParser.AllowRuleContext ctx) {
            this.subjectBuilder = builder.grant();
        }

        @Override
        public void enterPrincipalType(RuleParser.PrincipalTypeContext ctx) {
            var principalClass = lookupClass(ctx.IDENT(), Principal.class, "Principal");
            this.principalBuilder = subjectBuilder.toSubjectsHavingPrincipal(principalClass);
            subjectBuilder = null;
        }

        @NonNull
        private <T> Class<? extends T> lookupClass(TerminalNode node, Class<T> cls, String desc) {
            // look it up in the imports
            String localClassName = node.getText();
            var qualifiedClassName = localToQualified.get(localClassName);
            if (qualifiedClassName == null) {
                reportError(node.getSymbol(), "%s class with name '%s' has not been imported.".formatted(desc, localClassName));
            }
            try {
                Class<?> c = Class.forName(qualifiedClassName);
                if (!cls.isAssignableFrom(c)) {
                    reportError(node.getSymbol(), "%s class '%s' is not a subclass of %s.".formatted(desc, localClassName, cls));
                }
                return c.asSubclass(cls);
            }
            catch (ClassNotFoundException e) {
                reportError(node.getSymbol(), "%s class '%s' was not found.".formatted(desc, qualifiedClassName));
            }
            return null;
        }

        @Override
        public void enterEqOp(RuleParser.EqOpContext ctx) {
            if (resourceBuilder != null) {
                resourceBuilder.forResourceWithNameEqualTo(unwrap(ctx.nameEq().STRING()));
                resourceBuilder = null;
            }
            else {
                this.operationsBuilder = principalBuilder.withNameEqualTo(unwrap(ctx.nameEq().STRING()));
                this.principalBuilder = null;
            }
        }

        @Override
        public void enterInOp(RuleParser.InOpContext in) {
            if (resourceBuilder == null) {
                reportError(in.nameIn().IN().getSymbol(), "'in' operation not supported on principals");
            }
            var names = in.nameIn().STRING().stream()
                    .map(this::unwrap)
                    .collect(Collectors.toSet());
            resourceBuilder.forResourcesWithNameIn(names);
            resourceBuilder = null;
        }

        @Override
        public void enterLikeOp(RuleParser.LikeOpContext like) {
            if (resourceBuilder == null) {
                reportError(like.nameLike().LIKE().getSymbol(), "'like' operation not supported on principals");
            }
            var pattern = unwrap(like.nameLike().STRING());
            int firstStartIndex = pattern.indexOf('*');
            if (firstStartIndex == -1) {
                resourceBuilder.forResourceWithNameEqualTo(pattern);
                resourceBuilder = null;
            }
            else if (firstStartIndex != pattern.length() -1) {
                reportError(like.nameLike().STRING().getSymbol(), "Wildcard '*' only supported as last character in 'like'");
            }
            else {
                resourceBuilder.forResourcesWithNameStartingWith(pattern.substring(0, firstStartIndex));
                resourceBuilder = null;
            }
        }

        @Override
        public void enterMatchOp(RuleParser.MatchOpContext match) {
            if (resourceBuilder == null) {
                reportError(match.nameMatch().MATCHING().getSymbol(), "'matching' operation not supported on principals");
            }
            String p = unwrap(match.nameMatch().REGEX());
            try {
                Pattern.compile(p); // check it's valid
            }
            catch (PatternSyntaxException e) {
                reportError(match.nameMatch().REGEX().getSymbol(), "Regex provided for 'matching' operation is not valid: %s".formatted(
                        e.getMessage()));
            }
            if (resourceBuilder != null) {
                resourceBuilder.forResourcesWithNameMatching(p);
                resourceBuilder = null;
            }
        }

        @Override
        public void enterWildcardOp(RuleParser.WildcardOpContext ctx) {
            if (resourceBuilder == null) {
                reportError(ctx.STAR().getSymbol(), "wildcard operation '*' not supported on principals");
            }
            resourceBuilder.forAllResources();
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
            var enumCls = cls.asSubclass(Enum.class);
            if (allOps) {
                this.resourceBuilder = operationsBuilder.allOperations((Class) enumCls);
            }
            else {
                var list = (List) opNames.stream().map(name -> Enum.valueOf(enumCls, name)).toList();
                this.resourceBuilder = operationsBuilder.operations(EnumSet.copyOf(list));
            }
            this.operationsBuilder = null;
            this.opNames = null;
            this.allOps = false;
        }
    }
}
