/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.maven.enforcer.rule.api.AbstractEnforcerRule;
import org.apache.maven.enforcer.rule.api.EnforcerRuleException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.execution.ProjectDependencyGraph;
import org.apache.maven.project.MavenProject;

/**
 * Custom Enforcer Rule
 */
@Named("customRule") // rule name - must start from lowercase character
public class EnforcerCustomRule extends AbstractEnforcerRule {
    // Inject needed Maven components
    @Inject
    private MavenProject project;

    @Inject
    private MavenSession session;

    public void execute() throws EnforcerRuleException {

        List<String> modules = project.getModules();
        boolean systemTestIsTheLast = modules.get(modules.size() - 1).toLowerCase().contains("systemtest");
        ProjectDependencyGraph graph = session.getProjectDependencyGraph();
        List<MavenProject> projects = graph.getAllProjects();
        getLog().info("Maven projects " + Arrays.toString(projects.toArray()));

        if (systemTestIsTheLast) {
            throw new EnforcerRuleException("Failing because system test module is not the last, and could be used as a dependency in another project");
        }
    }
}
