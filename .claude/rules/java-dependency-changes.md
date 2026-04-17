---
paths:
  - "**/pom.xml"
---

# Java Dependency Change Requirements

Follow this guidance when adding or changing Java dependencies.

## Open Source only

This is a strictly open source project.
This extends to our direct and transitive dependencies. 
We can only add a new dependency (or upgrade an existing dependency) when the dependency, and all its transitive dependencies, have an open source license. 
Open source license means a license that's approved but the OSI. 
We prefer non-copyleft open source licenses. 

## Adhere to the license conditions

Evaluate what steps need to be taken to comply with the condition of the license, and follow them.

## Minimise the number of dependencies

We want to avoid having a larger set of dependencies than is strictly necessary, because:
* Dependencies are a liability as much as an asset. 
* They can increase our support burden by requiring dependency upgrades.
* The can also increase our users' exposure to security threats.
* Even when a dependency's CVE is not expoitable in the proxy we have to fix it in practice because of false positives from security scanning software.

The following criteria should be considered:

* Where functionality is simple we prefer to implement it in our own codebase, rather than using a dependency to provide it.
* All else being equal, we prefer libraries that have no dependencies of their own.
  Failing that, libraries that require few dependencies of their own are preferable.
* Libraries that are well-known and/or obviously well-maintained are preferred.
    * Regular releases are one signal of "well-maintained".
* We prefer reusing an existing dependency over adding a new dependency with similar function.
  For example, don't add a dependency on a YAML parser without a compelling reason;
  you preferring that library over Jackson is not a compelling reason.
* Dependencies with native code require special consideration: bring this up directly with the project managers.
* Be extra cautious if a proposed dependency has a version clash with an existing dependenency.
  There are few guardrails in this situation, so we need to evaluate how well the troublesome dependency manages binary compatibility.

In practice the standard is not quite as high for dependencies which will not make it into the production code.
