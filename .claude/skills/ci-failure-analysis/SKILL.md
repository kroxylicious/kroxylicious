---
name: ci-failure-analysis
description: Analyze a failed GitHub Actions CI run, extract the failing Surefire/Failsafe test, check for duplicate issues, and (with confirmation) file an issue and/or comment on the PR
---

## CI Failure Analysis

`/ci-failure-analysis <run-url-or-id> [--rerun]`

Input is either a full run URL (`https://github.com/<owner>/<repo>/actions/runs/<id>`)
or a bare run ID (owner/repo then default to `git remote get-url origin`).

1. **Fetch**
   - `gh run view <id> -R <owner>/<repo> --json conclusion,headBranch,headSha,jobs,url,event,workflowName`
   - `gh run view <id> -R <owner>/<repo> --log-failed` (failing steps only, not the full log)

2. **Extract** — find `Tests run: X, Failures: Y, Errors: Z, Skipped: W` summaries and
   the `<<< FAILURE!` / `<<< ERROR!` markers (format `method(Class)`) with their stack
   traces. If there's no recognizable Surefire/Failsafe failure, say so rather than
   forcing a guess.

3. **Search for duplicates** —
   `gh issue list -R <owner>/<repo> --search "<Class> <method>" --state all --json number,title,state,url,labels`.
   Search by text only — never filter or guess by label name, the label taxonomy isn't fixed.

   Steps 1-3 are mechanical and cheap. Do them via the `Agent` tool with
   `model: "haiku"`, passing steps 1-3 above verbatim as the prompt (plus the
   resolved owner/repo/run-id). Require JSON output only, one object per failed
   test: `{testClass, testMethod, stackTrace, jobUrl, candidateDuplicates: [{number, title, url}]}`.

4. **Duplicate handling**
   - No real duplicate → read `.github/ISSUE_TEMPLATE/flaky_test.md` fresh each time
     (don't hardcode its title pattern/labels, it can change) and fill it in. Show the
     exact title/labels/body and get explicit confirmation before `gh issue create`.
   - Real duplicate that is **open** → draft a comment on it with the new run/job link
     and stack trace. Show the exact text and get explicit confirmation before
     `gh issue comment <number>`.
   - Real duplicate in any other state (closed, etc.) → judgement call. Surface it and
     ask rather than acting automatically.

5. **Comment on the PR** — if the run belongs to a PR (`event == pull_request`, or look one
   up via `gh pr list -R <owner>/<repo> --head <headBranch>`), draft a short comment
   (failing test, job link, duplicate-or-filed issue) and get explicit confirmation before
   `gh pr comment`.

6. **Rerun** — only if `--rerun` was passed or explicitly requested:
   `gh run rerun <id> -R <owner>/<repo> --failed`.

### Rules

- Never run `gh issue create`, `gh issue comment`, `gh pr comment`, or `gh run rerun`
  without explicit confirmation of the exact text/action first.
- Never assume issue labels — read them from the template each time.
