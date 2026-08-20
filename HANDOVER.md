# Handover: PLAN2.md kafka-vendoring migration, in progress

Branch: `kafka-vendoring-phase-d`. Read `PLAN2.md` first (governing plan), then this file
(what actually happened vs. the plan, and exact next steps). Delete this file once the
migration is complete and merged.

## Current state: what's committed vs. uncommitted

**Committed** (on `kafka-vendoring-phase-d`, in order): everything through
`kroxylicious-filters/*` (all 11 submodules), commit `2b78ab486`. See `git log` for the full
list — unchanged from before this session.

**Uncommitted, working-tree changes present right now** (~180 files touched, not yet
`git add`/committed). This session's work landed on top of the previous session's uncommitted
diff. Breakdown:

### Previous session's uncommitted work (unchanged this session, still needs review + commit)
- `kroxylicious-integration-test-support` (main + test) — migrated and verified.
- `kroxylicious-runtime-plugins` — only `kroxylicious-filter-archetype` needed changes; the
  other 9 submodules had zero `org.apache.kafka.*` references, untouched.
- `kroxylicious-filter-archetype` — template files migrated. **Still not independently
  compiled** (the archetype's `integration-test` goal fails in this environment on an
  unrelated, pre-existing offline-mode issue resolving `com.tisonkun.os:os-detector-maven-plugin`
  — not caused by this migration). Low risk, same pattern as every other filter module, but
  flagged as unverified.
  - **Also found this session**: an untracked stray file `kroxylicious-filter-archetype/.pom.xml.swp`
    (an editor swap file, not from any migration script). Not part of the migration — delete it
    or check where it came from before committing, don't let it get swept into `git add`.
- `kroxylicious-microbenchmarks` (main) — migrated and verified.
- `kroxylicious-app` — no changes needed, verified clean.
- `kroxylicious-krpc-plugin` — migrated and verified (`KrpcGeneratorTest` passes against the
  rewritten golden `.txt` fixtures).

### This session's work
1. **`kroxylicious-integration-tests` — now fully test-compiles clean.** This was the item
   the previous handover left as "next step." All 17 files that failed to compile are fixed.
   Root cause for all of them: the bulk §9 `migrate.sh` script rewrote `TopicPartition`,
   `Uuid`, and `Header`/`RecordHeader` wholesale to the vendored
   `io.kroxylicious.kafka.common.*` types, including call sites that pass these values into
   **kafka-clients client API** methods (`KafkaConsumer`, `KafkaProducer`, `AdminClient`,
   `ConsumerRecords`, `ConsumerRebalanceListener`, `OffsetAndMetadata` maps, `ProducerRecord`).
   Per PLAN2.md §6, those call sites must stay on `org.apache.kafka.common.*` — only usages
   that flow into/out of the vendored `*Data` message world should be vendored. None of the
   fixed files in this module have any message-level usage of `TopicPartition`/`Uuid`/`Header`
   at all, so every fix was a straight import swap back to the kafka-clients type (no file
   needed both types side by side — simpler than the dual-import pattern already used
   elsewhere in this module, e.g. `AbstractFilterIT.java`'s `toVendoredUuid` helper).
   - 11 files fixed by a background agent (before being interrupted, see Housekeeping):
     `AbstractFilterIT.java`, `ApiVersionsDowngradeIT.java`, `PluginTlsApiIT.java`,
     `AuthzIT.java`, `ClusterPrepUtils.java`, `GroupTracingIT.java`, `ProduceAuthzIT.java`,
     `ProduceAuthzTxnlIdIT.java`, `TopicTracingIT.java`, `TransactionalIdTracingIT.java`
     (`AbstractTracingIT.java` turned out already fine — only touched by a deprecation
     warning, not an error).
   - 6 files fixed directly this session:
     - `EntityIsolationIT.java`, `filter/multitenant/MultiTenantIT.java`,
       `filter/multitenant/BaseMultiTenantIT.java` — `TopicPartition` import swapped from
       vendored back to `org.apache.kafka.common.TopicPartition`. This also transitively fixed
       the `PartitionAssignmentAwaitingRebalanceListener` nested class in `EntityIsolationIT`/
       `MultiTenantIT` (it implements `ConsumerRebalanceListener`, which requires the
       kafka-clients `TopicPartition` in its method signatures to actually override — no
       separate structural fix was needed once the import was corrected).
     - `filter/encryption/RecordEncryptionDeserializationCompatibilityIT.java`,
       `filter/encryption/RecordEncryptionFilterIT.java` — `Header`/`RecordHeader` imports
       swapped from vendored back to `org.apache.kafka.common.header.{Header,internals.RecordHeader}`.
       Used exclusively to build `ProducerRecord`s and read `ConsumerRecord.headers()` —
       client-side KEEP, not the vendored record-internal `Header` used by the record-encryption
       *filter's own* main-source record processing (that stays vendored, untouched).
     - `filter/validation/JwsSignatureRecordValidationIT.java` — same `RecordHeader` swap,
       same reason (`ProducerRecord` construction only).
   - Verified: `mvn -o -pl kroxylicious-integration-tests test-compile` → `EXIT=0`, checkstyle
     and the formatter/import-sort plugins all pass clean too (this module runs those in the
     `test-compile` lifecycle).

2. **Ran the PLAN2.md §11 step 9 confinement checks across the whole reactor.** Both pass, with
   two things worth knowing:
   - The broad check (`org.apache.kafka.common.{protocol.ApiKeys|Errors,errors.,requests.}`
     outside the vendored tree) turns up **three** files, not the two PLAN2.md predicted:
     - `kroxylicious-runtime/.../internal/KafkaProxyExceptionMapper.java` — expected (§8.6).
     - `kroxylicious-filters/kroxylicious-authorization/.../RequestDataUtils.java` — a Javadoc
       `{@code ...}` mention of the class name only, not a real import. Harmless, same category
       as the `SendBuilder` Javadoc note in PLAN2.md §6. No action needed.
     - `kroxylicious-filter-test-support/.../assertj/ResponseAssert.java` — a **genuine,
       previously-undocumented hot spot**, not flagged by any prior session. It round-trips a
       vendored response through kafka-clients' `AbstractResponse.parseResponse(...)` to reuse
       `errorCounts()` (no vendored equivalent exists), translating `ApiKeys`/`Errors` via
       `.id`/`.code()` — the same pattern as §8.6. Already has an explanatory comment in the
       code. **No change needed**, just noting it exists so nobody "fixes" it by mistake later.
       Worth adding as a third expected exception if PLAN2.md's confinement-check list is ever
       updated.
   - `OauthBearerValidationFilter.java` — the open question from the prior handover ("does it
     need the same §8.6 treatment?") is **resolved: no**. It only uses the vendored
     `SaslAuthenticationException` and KEEP `org.apache.kafka.common.security.oauthbearer.*`
     client classes — no `requests.*`/`AbstractResponse` dependency at all.
   - The stricter `kroxylicious-api`-only check (zero `org.apache.kafka.*` outside the vendored
     tree, zero `kafka-clients` in the pom) is clean.

3. **Found and fixed two real (non-mechanical) issues while attempting a full reactor
   `install`** (PLAN2.md §11 step 4). These are **new discoveries this session**, not
   anticipated by PLAN2.md, surfaced by Maven's `dependency:analyze-only` and the per-module
   checkstyle "layering" (`ImportControl`) execution — both of which are part of `mvn install`/
   `verify` but not part of a bare `compile`/`test-compile`, which is why they weren't caught
   earlier:
   - `kroxylicious-filters/kroxylicious-record-encryption/pom.xml` — **removed the main-scope
     `kafka-clients` dependency.** This module's main and test sources were already fully
     migrated to vendored types in the earlier `2b78ab486` commit (verified: zero
     `org.apache.kafka.*` references anywhere under `src/main` or `src/test`), so the declared
     dependency had become fully unused and `dependency:analyze-only` correctly flagged it.
     This goes slightly beyond PLAN2.md's explicit scope (only `kroxylicious-api` was targeted
     for `kafka-clients` removal), but it's the correct, low-risk cleanup given the audit.
   - `kroxylicious-filters/kroxylicious-record-encryption/etc/module-layering.xml` — this
     module has a per-package `ImportControl` checkstyle config (`etc/module-layering.xml`)
     that allowlists importable packages per subpackage. It still listed
     `org.apache.kafka.common.*` (message/record/errors/utils/protocol/header) from before the
     migration; the actual source files were already migrated to `io.kroxylicious.kafka.common.*`
     in the earlier commit, so every vendored import was failing the "layering" checkstyle
     execution (61 errors). Fixed with a straight `org.apache.kafka.common` →
     `io.kroxylicious.kafka.common` string rewrite across the whole file — checkstyle's
     `ImportControl` `<allow pkg="...">` matches subpackages by default (no `exact-match`
     attribute set), so e.g. allowing `io.kroxylicious.kafka.common.record` also covers
     `io.kroxylicious.kafka.common.record.internal.*`, same as the old rule covered
     `org.apache.kafka.common.header.internals.*` under a plain `org.apache.kafka.common.header`
     allow. **Verified this is the only module-layering.xml in the repo with this problem**
     (`grep -rl "org.apache.kafka" --include="module-layering.xml"` across
     `kroxylicious-filters kroxylicious-runtime kroxylicious-api kroxylicious-filter-test-support`
     found only this one file) — no other module needs the same fix.
   - `kroxylicious-runtime/pom.xml` — added a missing **test-scope** dependency on
     `kroxylicious-kafka-message-json`. `kroxylicious-runtime/src/test/java/io/kroxylicious/proxy/internal/filter/BrokerAddressFilterTest.java`
     imports `io.kroxylicious.kafka.common.message.json.KafkaApiMessageConverter` from that
     module but was resolving it transitively (via `kroxylicious-filter-test-support`, most
     likely) rather than declaring it directly — `dependency:analyze-only` flagged it as "used
     undeclared". Straightforward fix, same category as PLAN2.md §2d's other consumer-wiring
     steps for the new JSON module, just one the plan's own audit of consumers missed.
   - After both fixes, `kroxylicious-record-encryption`'s own `mvn verify` (checkstyle +
     dependency:analyze, with japicmp/spotbugs/revapi/javadoc skipped) passes clean.

## What's NOT done yet — exact next steps for a new session, in order

1. **Re-run the full reactor install** that was interrupted mid-way this session (a maven
   command got cancelled by the user right as it was about to run, no output to distrust —
   it just didn't execute):
   ```
   mvn -o -DskipTests -Dmaven.test.skip=true -Dspotbugs.skip=true -Djapicmp.skip=true \
     -Drevapi.skip=true -Dmaven.javadoc.skip=true -Darchetype.test.skip=true install \
     > /tmp/krox-install-full2.log 2>&1; echo "EXIT=$?"
   ```
   This picks up the `kroxylicious-runtime/pom.xml` fix (item 3 above) which was made but not
   yet re-verified with a full reactor build. **Do this as a single top-level `mvn install`,
   not `-am` module-by-module slices** — per user instruction this session, `-am` rebuilds are
   expensive; prefer installing once with quality gates trimmed down (`-Dquick` alone is not
   enough — it does not set `checkstyle.skip`; use the explicit skip flags above, or add
   `-Dcheckstyle.skip=true` too if you want a maximally fast sanity pass and will run checkstyle
   separately) and then run any per-module command directly against the module (`-pl <module>`,
   no `-am`) since everything is already installed to `~/.m2`.
   - Read the errors if any. Given this session's fixes, expect it to either succeed or surface
     one or two more of the same two error classes (`dependency:analyze-only` /
     checkstyle `layering`) in a module not yet checked — the fix pattern is now established
     (see item 3 above), apply the same reasoning: is the dependency genuinely unused? is a
     `module-layering.xml`/`import-control` file stuck on the old package name?
2. **Known, deliberately-unresolved item — do not silently work around it**: `japicmp` fails
   `mvn install`/`verify` on `kroxylicious-api` because `RequestFilter`/`ResponseFilter`'s
   public method signatures now use vendored types instead of kafka-clients types — a genuine
   breaking API change. This needs a maintainer decision (bump the reference version being
   compared against, or add explicit `<excludes>` to the japicmp config) — not a mechanical
   fix. Every install command above uses `-Djapicmp.skip=true` to route around it for now;
   don't let that skip flag quietly disappear into a real PR without calling this out.
3. Once the full reactor installs clean (with the skip flags), decide whether to also run a
   real `test-compile` sweep per PLAN2.md §11 step 8 across modules not already verified this
   session or the last (most have already been verified — see the per-module summaries above
   and in `PLAN2.md` §7 for the full module list; the main candidates left are the ones never
   explicitly re-verified after this session's pom/config changes, i.e. re-check
   `kroxylicious-runtime` test-compile since its pom changed).
4. Delete the stray `kroxylicious-filter-archetype/.pom.xml.swp` file (or otherwise resolve it)
   before staging anything — it's untracked, not part of the migration, and shouldn't be
   accidentally committed.
5. Review and commit the large uncommitted diff (~180 files across 9 modules once you add this
   session's 3 files). Given the size, split into a few logical commits rather than one, e.g.:
   - `integration-test-support` + `microbenchmarks` + `app` (all verified clean, from the prior
     session)
   - `krpc-plugin` (verified clean, from the prior session)
   - `filter-archetype` (unverified — flag it in the commit body)
   - `integration-tests` (compile-verified this session)
   - `record-encryption` pom/layering fix + `runtime` pom fix (this session's two real,
     non-mechanical fixes — probably worth its own small commit with a clear message about
     *why*, since these are genuine dependency/config corrections, not mechanical import
     rewrites)
   Match the commit-message style of the prior commits on this branch
   (`feat(kafka-vendoring): migrate <module>`, `Assisted-by:` trailer, DCO signoff via the
   repo's git hook).
6. After everything above, PLAN2.md's module list (§7) is fully covered and the confinement
   checks (§11 step 9) already pass as of this session. The only remaining open item before
   this migration can be considered "done" per PLAN2.md's stated goal (compile, not full
   correctness) is the japicmp API-change decision in item 2.

## Housekeeping

- `/tmp/migrate.sh` has the §9 bulk-rewrite script, used throughout prior sessions, matches
  PLAN2.md §9 exactly. Re-create it from the plan if working on a different machine.
- **On background agents**: this session delegated the `kroxylicious-integration-tests` fix to
  a forked background agent. Its first run reported completion after 0 tool calls in ~3.6s —
  a false-completion signal, not real work (confirmed by checking `git diff` afterward: no
  changes from that run). It was resumed with an explicit instruction to actually do the work,
  made real progress (fixed 10 of 17 files), then was killed by an unrelated user action (a
  `/usage` command) partway through. The remaining 7 files were finished by hand in the main
  session (one, `AbstractTracingIT.java`, turned out not to need any fix). If delegating
  similar mechanical-but-bulky fix-up work to a background agent again, verify its diff
  independently before trusting a "done" report — don't assume 0 tool calls means anything
  other than "didn't do the work."
- Per repo convention (`/home/robeyoun/.claude/rules/03-maven.md`), always pipe `mvn` output to
  a `/tmp/*.log` file and grep it, rather than reading raw stdout — these logs are large.
- Per user instruction this session: avoid repeated `-am` (also-make) Maven builds for
  iteration — they rebuild the whole upstream reactor every time and are expensive. Prefer:
  install once (`mvn install` with quality gates trimmed, see item 1 above), then target
  individual modules directly with `-pl <module>` (no `-am`) for fast iteration once their
  dependencies are already in `~/.m2`.
