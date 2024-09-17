### Type of change

_Select the type of your PR_

- Bugfix
- Enhancement / new feature
- Refactoring
- Documentation

### Description

_Please describe your pull request_

### Additional Context

_Why are you making this pull request?_

### Checklist

_Please go through this checklist and make sure all applicable tasks have been done_

- [ ] Write tests
- [ ] Update documentation
- [ ] Make sure all unit/integration tests pass
- [ ] Make sure all Sonarcloud warnings are addressed or are justifiably ignored.
- [ ] If applicable to the change, [trigger the system test suite](../blob/main/DEV_GUIDE.md#jenkins-pipeline-for-system-tests).  Make sure tests pass.
- [ ] If applicable to the change, [trigger the performance test suite](../blob/main/PERFORMANCE.md#jenkins-pipeline-for-performance). Ensure that any degradations to performance numbers are understood and justified.
- [ ] Ensure the PR references relevant issue(s) so they are closed on merging.
- [ ] For user facing changes, update CHANGELOG.md (remember to include changes affecting the API of the test artefacts too).

> **_NOTE:_**  You must be a member of `@kroxylicious/developers` to trigger the system test and performance test suites.  If you are not part of this group, comment on the PR requesting a trigger, tagging `@kroxylicious/developers`.
