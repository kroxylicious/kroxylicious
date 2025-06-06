# Doxylicious

## What is this?

Kroxylicious has adopted [modular documentation](https://redhat-documentation.github.io/modular-docs/#) as a way of structuring content for end-users.
[_Procedures_](https://redhat-documentation.github.io/modular-docs/#con-creating-procedure-modules_writing-mod-docs) are a particular kind of documentation module that explains how  to perform a specific task. 
Procedures have a prescriptive structure, which includes:

* listing prerequisite procedures,
* the steps to be taken in this procedure,
* optionally, steps to be taken to verify that this procedure worked.

Procedures need to be tested to confirm that the documented steps actually work.

This module enables defining a procedure in a form which can be used both
* to generate the procedure documentation, in AsciiDoc 
* to execute the procedure as a test, using the Junit 5 framework

In order words, this makes it possible to automatically test some of your documentation, eliminating toil and the possibility of divergence between what's documented and what actually works.

## Modules 

* `doxylicious-core` defines all the core functionality.
* `doxylicious-junit-provider` implements a Junit5 extension to enable execution of procedures as tests.
* `doxylicious-maven-plugin` implements a Maven plugin for generating the procedure AsciiDocs.

## Machine-readable procedures

AsciiDoc is a fine format for authoring documentation, but was never intended to be executable. 
These tools instead accept procedures defined in YAML (or JSON if you prefer) as a single source of truth for procedures.
The AsciiDoc form of the procedure can be generated from the YAML.

### A basic proc

Let's start with a simple procedure

```yaml
id: succeeding_reliably # <1>
title:
  adoc: Succeeding reliably # <2>
abstract:
  adoc: 'This procedure is a minimal example.' # <3>
intro:
  adoc: |- # <4>
    This procedure will always succeed because the only thing actually executed is the command `true`, which always returns 
     with a 0 error code.
procedure:
  - step: # <5>
      - adoc: Execute `true`.
      - exec: /usr/bin/true
      - adoc: It is expected that this command returns without any output.
  - step:
      - adoc: Drink some coffee.
verification:
    - step: # <6>
          - adoc: Check that coffee was drunk.
          - exec: /usr/bin/true
```

1. Each procedure must have an `id`. This id needs to be unique within all the procedures defined in both `src/main/proc` and `src/test/proc`.
2. Published procedures¹ must have an AsciiDoc `title`.
3. Published procedures may have an AsciiDoc `abstract`.
4. Published procedures may have an AsciiDoc `intro`.
5. This procedure has two steps, the first of which has three substeps. A substep:
    * either specifies some words, using `adoc`,
    * or specifies a command to be executed, using `exec`. If the process running the tests has a sensible `$PATH` there's no need to use absolute paths in the command of an `exec` substep.
6. Optionally, procedures may have `verification` steps.

<small>¹ See the section on generating documentation for what 'published' means.</small>

### A proc with prerequisites

```yaml
id: defining_prereqs 
title:
  adoc: Defining prerequisites
abstract:
  adoc: 'This procedure demonstrates prerequisites.' 
prereqs: # <1>
  - ref: succeeding_reliably # <2>
    adoc: You should have already succeeded # <3>
procedure:
  - step: # <5>
      - adoc: Execute `true` again
      - exec: true
```

1. A proc provides a list of other procedures that must be done before this one. 
2. The `ref` is the id of the prerequisite procedure.
3. If this procedure is published, it also must define an `adoc` with the words describing the prerequisite.

When `defining_prereqs` is tested the test framework will automatically execute `succeeding_reliably` first. In the terminology of software testing, each prerequisite is a [_test fixture_](https://en.wikipedia.org/wiki/Test_fixture#Software) of the procedure under test.

When AsciiDoc for `defining_prereqs` is generated the prerequisites will be included.

### Notional procedures

Sometimes there are different ways of doing a thing. 
For example, in describing how a piece of software might be is installed we might be able to download the binary, or we might install it using a package manager such as `dnf`.
Subsequent procedures which describe how to use the software don't care _how_ the software was installed, only that it _was_ installed. 

To model this we have the concept of a _notional_ procedure.

```yaml
id: installing_the_thing
notional: true
```

A notional procedure doesn't define any `procedure` property and cannot itself be executed. 
However, it can be _satisfied_ by one or more other procedures. 

```yaml
---
id: installing_the_thing_from_the_website
satisfies: installing_the_thing
# ... title, procedure, etc
---
id: installing_the_thing_using_dnf
satisfies: installing_the_thing
# ... title, procedure, etc
```

Aside: `notional` and `satisfies` are conceptually the same as `interface` and `implements` in the Java programming language. Procedures already use the term `abstract`, in the sense of a summary, so different terminology is used to avoid ambiguity.

When testing, a `notional` procedure can be replaced by any of the procedures which satisfy it. 
By using a `notional` prerequisite procedure it's possible to execute the same procedure multiple times, but with the prerequisite satisfied in different ways.

```yaml
id: using_the_thing
prerequisites:
  - ref: installing_the_thing
# ...
```

In this example, `using_the_thing` can be tested twice: once with the `installing_the_thing` prerequisite satisfied by `installing_the_thing_from_the_website`, and again with it satisfied by `installing_the_thing_using_dnf`. You can choose to execute tests covering all the available satisfying procedures, or to select a subset.

## Testing

### Configure the dependency

Add the dependency in `test` scope, in the `project/dependencies` of your `pom.xml`:
```xml
<dependency>
    <groupId>io.kroxylicious</groupId>
    <artifactId>doxylicious-junit-provider</artifactId>
    <version>${project.version}</version>
    <scope>test</scope>
</dependency>
```

### Write your procedure YAMLs

Put them in the `src/test/proc` directory.

### Write your test classes

The extension works with `@TestTemplate`-annotated methods and provides
an argument for test method parameters with type `Procedure`.
The extension knows about all the procedures defined in `src/main/proc` and `src/test/proc`.
Those procedures will be validated by the extension to detect things like procedures with cyclic definitions.
The method must also be annotated with `@TestProcedure` so the extension knows  which procedure is being tested.
The extension will arrange for the execution of the test procedure's transitive `prerequisites` before entry to the test method, and also for the execution of `undo` steps of the test procedure and its transitive prerequisites. 

The test method will be invoked multiple times, once for each of the possible ways the test procedure and its prerequisites can be satisfied. 

The `@TestProcedure` annotation can also be used to skip execution of some of the prerequisites of the test procedure (for example if the test class itself is honoring a prerequisite via a `@BeforeEach`-annotated method).

The `Procedure` object can be used to execute the `procedure` and to assert that its `verification` is successful.

```java
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.doxylicious.junit5.*;

class ExampleTest {

    @TestProcedure("notional_proc")
    void test(Procedure procedure) {
        // when
        procedure.executeProcedure();

        // then
        procedure.assertVerification();
    }
}
```

## Generating documentation

### Configuring the Maven plugin

Declare the plugin in the `project/build/plugins` of your `pom.xml`: 

```xml
<plugin>
    <groupId>io.kroxylicious</groupId>
    <artifactId>doxylicious-maven-plugin</artifactId>
    <version>${doxylicious-maven-plugin.version}</version>
    <executions>
        <execution>
            <goals>
                <goal>generate</goal>
            </goals>
            <configuration>
                <procsDir>src/main/proc</procsDir>
                <testProcsDir>src/test/proc</testProcsDir>
                <targetDir>../docs/modules</targetDir>
            </configuration>
        </execution>
    </executions>
</plugin>
```

And define the version of the plugin to use in the `project/properties` of your `pom.xml`.

### What gets documented?

Not all procedures need to be available to end users in documentation.
This can often be the case for prerequisite procedures which are needed as test fixtures, but which you don't want to be part of the published documentation. 

The maven plugin takes a very simple approach to deciding which procedures should have AsciiDoc generated for them:

* those in the configured `procsDir` (or `src/main/proc` by default) are included,
* those in the configured `testProcsDir` (or `src/test/proc` by default) are excluded.

Ultimately, whether the generated procedures are actually published depends on them being referenced from the rest of your documentation using AsciiDoc's `include` directive.