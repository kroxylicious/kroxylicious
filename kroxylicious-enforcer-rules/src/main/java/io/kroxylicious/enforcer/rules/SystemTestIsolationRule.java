/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.enforcer.rules;

import java.util.Arrays;
import java.util.List;

import javax.inject.Named;

import org.apache.maven.enforcer.rule.api.EnforcerRule;
import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.enforcer.rule.api.EnforcerRuleHelper;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.execution.ProjectDependencyGraph;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.component.configurator.expression.ExpressionEvaluationException;

/**
 * System test isolation rule
 * Checks that no other modules depends on System test module
 */
@Named("systemTestIsolationRule") // rule name - must start from lowercase character
public class SystemTestIsolationRule implements EnforcerRule {

    public void execute(EnforcerRuleHelper helper) throws EnforcerRuleException {
        final Log log = helper.getLog();
        final MavenProject currentProject;
        final MavenSession session;
        try {
            currentProject = ((MavenProject) helper.evaluate("${project}"));
            session = (MavenSession) helper.evaluate("${session}");
        }
        catch (ExpressionEvaluationException e) {
            throw new RuntimeException(e);
        }

        ProjectDependencyGraph graph = session.getProjectDependencyGraph();
        List<MavenProject> downProjects = graph.getDownstreamProjects(currentProject, true);
        log.info("Maven downstream projects: " + Arrays.toString(downProjects.toArray()));

        if (!downProjects.isEmpty()) {
            throw new EnforcerRuleException("Failing because system test module is used as a dependency in another project");
        }
    }

    @Override
    public boolean isCacheable() {
        return false;
    }

    @Override
    public boolean isResultValid(EnforcerRule enforcerRule) {
        return false;
    }

    @Override
    public String getCacheId() {
        return null;
    }
}
