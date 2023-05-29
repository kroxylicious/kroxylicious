window.BENCHMARK_DATA = {
  "lastUpdate": 1685395724465,
  "repoUrl": "https://github.com/kroxylicious/kroxylicious",
  "entries": {
    "kafka producer perf test Benchmark": [
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "f0b02f1eda38d778dfedb0df35b7647f796a3159",
          "message": "Change performance tests results branch to performance-results\n\nWhy:\nPerformance benchmark results are wiped from the gh-pages branch by the asciidoc\naction when commits are pushed to main. The asciidoctor-ghpages-action action\nrebases the branch down to a single commit and blows away other changes. So commits\nfrom the performance tests are lost.\n\nBy writing to the performance-results branch we will keep historical results but the\nnice graphs won't be available in github pages. We can check them out locally to view\nthem.",
          "timestamp": "2023-03-08T22:50:35Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/f0b02f1eda38d778dfedb0df35b7647f796a3159"
        },
        "date": 1678331059167,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 10.16,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 5,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 376,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "f0b02f1eda38d778dfedb0df35b7647f796a3159",
          "message": "Change performance tests results branch to performance-results\n\nWhy:\nPerformance benchmark results are wiped from the gh-pages branch by the asciidoc\naction when commits are pushed to main. The asciidoctor-ghpages-action action\nrebases the branch down to a single commit and blows away other changes. So commits\nfrom the performance tests are lost.\n\nBy writing to the performance-results branch we will keep historical results but the\nnice graphs won't be available in github pages. We can check them out locally to view\nthem.",
          "timestamp": "2023-03-08T22:50:35Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/f0b02f1eda38d778dfedb0df35b7647f796a3159"
        },
        "date": 1678331776110,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 11.73,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 5,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 505,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "26e3071dd873fafdbb8673092a5b048c57463c81",
          "message": "Use a PR ref in the performance action when triggered by PR comment\n\nWhy:\nCurrently the action fails if you comment `/perf` on a PR that exists\nin a remote fork because the checkout action can't checkout the ref\nwe get from the pull-request-comment-branch action\n\nhttps://www.jvt.me/posts/2019/01/19/git-ref-github-pull-requests\n\ngithub creates two refs in the base repository for each PR:\nrefs/pull/123/head\nrefs/pull/123/merge\n\nWe can checkout refs/pull/123/head instead of using the github API to\nlookup the ref associated with the PR",
          "timestamp": "2023-03-09T08:29:28Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/26e3071dd873fafdbb8673092a5b048c57463c81"
        },
        "date": 1678409956414,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 140.39,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 606,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 664,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sam Barker",
            "username": "SamBarker",
            "email": "sam@quadrocket.co.uk"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "165b4c5555afd43509e3945fa83c3dcdca50270c",
          "message": "Merge pull request #78 from SamBarker/helloCheckstyle\n\nAdd checkstyle",
          "timestamp": "2023-03-10T01:14:09Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/165b4c5555afd43509e3945fa83c3dcdca50270c"
        },
        "date": 1678411415087,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 21.78,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 44,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 636,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "d331499423b49effe0e98c413319f1665407f8b6",
          "message": "Use new MethodSource syntax\n\nThere was a change of behaviour in 5.9.0 where methods with arguments became\nconsidered candidate factory methods causing it to throw exceptions when it was\nambiguous which method was targeted.\n\nhttps://github.com/junit-team/junit5/issues/3080\nhttps://github.com/junit-team/junit5/commit/825ea38857bff2dcbc200c6ceb7972dbc89482b0\nhttps://junit.org/junit5/docs/current/release-notes/index.html#bug-fixes-2",
          "timestamp": "2023-03-13T20:16:39Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/d331499423b49effe0e98c413319f1665407f8b6"
        },
        "date": 1678746059644,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 10.5,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 4,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 479,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "d096dab2d21cc5195df06c7b25188618213a2cbc",
          "message": "Always execute maven plugin to calculate rootdir\n\nWhy:\nWhen running `mvn clean verify -Dquick` the rootdir resolver plugin\nwas excluded due to the `quick` but the `formatter` plugin config depends\non `rootdir` being made available.\n\nThis prevented performance tests running.",
          "timestamp": "2023-03-13T22:10:34Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/d096dab2d21cc5195df06c7b25188618213a2cbc"
        },
        "date": 1678753686514,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 172.09,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 671,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 804,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "d096dab2d21cc5195df06c7b25188618213a2cbc",
          "message": "Always execute maven plugin to calculate rootdir\n\nWhy:\nWhen running `mvn clean verify -Dquick` the rootdir resolver plugin\nwas excluded due to the `quick` but the `formatter` plugin config depends\non `rootdir` being made available.\n\nThis prevented performance tests running.",
          "timestamp": "2023-03-13T22:10:34Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/d096dab2d21cc5195df06c7b25188618213a2cbc"
        },
        "date": 1678762929289,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 14.32,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 74,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 331,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "d096dab2d21cc5195df06c7b25188618213a2cbc",
          "message": "Always execute maven plugin to calculate rootdir\n\nWhy:\nWhen running `mvn clean verify -Dquick` the rootdir resolver plugin\nwas excluded due to the `quick` but the `formatter` plugin config depends\non `rootdir` being made available.\n\nThis prevented performance tests running.",
          "timestamp": "2023-03-13T22:10:34Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/d096dab2d21cc5195df06c7b25188618213a2cbc"
        },
        "date": 1678762957902,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 238.87,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 661,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 717,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "d096dab2d21cc5195df06c7b25188618213a2cbc",
          "message": "Always execute maven plugin to calculate rootdir\n\nWhy:\nWhen running `mvn clean verify -Dquick` the rootdir resolver plugin\nwas excluded due to the `quick` but the `formatter` plugin config depends\non `rootdir` being made available.\n\nThis prevented performance tests running.",
          "timestamp": "2023-03-13T22:10:34Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/d096dab2d21cc5195df06c7b25188618213a2cbc"
        },
        "date": 1678763092280,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 4.61,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 3,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 132,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Francisco Vila",
            "username": "franvila",
            "email": "57452611+franvila@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6bf98ac9add1ad8b01d108ea707ac079a90dc046",
          "message": "Fix performance action (#262)\n\n* finding the proper kroxy jar file\r\n\r\n* finding the proper kroxy jar file",
          "timestamp": "2023-04-11T07:02:47Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/6bf98ac9add1ad8b01d108ea707ac079a90dc046"
        },
        "date": 1681197023837,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 11.28,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 35,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 315,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Francisco Vila",
            "username": "franvila",
            "email": "57452611+franvila@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "6bf98ac9add1ad8b01d108ea707ac079a90dc046",
          "message": "Fix performance action (#262)\n\n* finding the proper kroxy jar file\r\n\r\n* finding the proper kroxy jar file",
          "timestamp": "2023-04-11T07:02:47Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/6bf98ac9add1ad8b01d108ea707ac079a90dc046"
        },
        "date": 1681203657151,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 263.63,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 653,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 781,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robeyoun@redhat.com"
          },
          "committer": {
            "name": "Robert Young",
            "username": "robobario",
            "email": "robertyoungnz@gmail.com"
          },
          "id": "88bfa9f750f56ae5e8f54e3107f393488fcad779",
          "message": "Wait for services in performance action\n\nWhy:\nWe saw a failure where kroxylicious was never started due to trying to\nreference a non-existent JAR. Because this was backgrounded it didn't\ncause the action to fail and the kafka producer performance script will\nretry forever, so the action eventually timed out after 6 hours. We want\nto fail faster to conserve action time and expose the issue.",
          "timestamp": "2023-04-11T21:26:19Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/88bfa9f750f56ae5e8f54e3107f393488fcad779"
        },
        "date": 1681383164377,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 32.92,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 217,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 476,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sam Barker",
            "username": "SamBarker",
            "email": "sam@quadrocket.co.uk"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9e537ed04b80f62017d50d9929f784dd7435eb26",
          "message": "Merge pull request #274 from kroxylicious/dependabot/maven/org.mockito-mockito-bom-5.3.0\n\nBump mockito-bom from 5.2.0 to 5.3.0",
          "timestamp": "2023-04-17T04:51:52Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/9e537ed04b80f62017d50d9929f784dd7435eb26"
        },
        "date": 1681717153979,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 10.9,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 5,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 444,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Sam Barker",
            "username": "SamBarker",
            "email": "sam@quadrocket.co.uk"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "3d0d80173b2a71f4bbfb270403db643a538efbf7",
          "message": "Merge pull request #297 from robobario/perf-test-git-details-fix\n\nRecord PR-comment triggered performance results against PR commit",
          "timestamp": "2023-05-01T04:14:03Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/3d0d80173b2a71f4bbfb270403db643a538efbf7"
        },
        "date": 1682914715606,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 19.33,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 115,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 360,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a48c56f4e2e70371222720b74ec20481e6e87a6e",
          "message": "Bump netty.version from 4.1.91.Final to 4.1.92.Final\n\nBumps `netty.version` from 4.1.91.Final to 4.1.92.Final.\n\nUpdates `netty-bom` from 4.1.91.Final to 4.1.92.Final\n- [Release notes](https://github.com/netty/netty/releases)\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.91.Final...netty-4.1.92.Final)\n\nUpdates `netty-codec-http` from 4.1.91.Final to 4.1.92.Final\n- [Release notes](https://github.com/netty/netty/releases)\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.91.Final...netty-4.1.92.Final)\n\nUpdates `netty-codec-haproxy` from 4.1.91.Final to 4.1.92.Final\n- [Release notes](https://github.com/netty/netty/releases)\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.91.Final...netty-4.1.92.Final)\n\n---\nupdated-dependencies:\n- dependency-name: io.netty:netty-bom\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n- dependency-name: io.netty:netty-codec-http\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n- dependency-name: io.netty:netty-codec-haproxy\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2023-05-01T04:23:28Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/a48c56f4e2e70371222720b74ec20481e6e87a6e"
        },
        "date": 1682915693194,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 20.92,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 97,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 511,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a48c56f4e2e70371222720b74ec20481e6e87a6e",
          "message": "Bump netty.version from 4.1.91.Final to 4.1.92.Final\n\nBumps `netty.version` from 4.1.91.Final to 4.1.92.Final.\n\nUpdates `netty-bom` from 4.1.91.Final to 4.1.92.Final\n- [Release notes](https://github.com/netty/netty/releases)\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.91.Final...netty-4.1.92.Final)\n\nUpdates `netty-codec-http` from 4.1.91.Final to 4.1.92.Final\n- [Release notes](https://github.com/netty/netty/releases)\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.91.Final...netty-4.1.92.Final)\n\nUpdates `netty-codec-haproxy` from 4.1.91.Final to 4.1.92.Final\n- [Release notes](https://github.com/netty/netty/releases)\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.91.Final...netty-4.1.92.Final)\n\n---\nupdated-dependencies:\n- dependency-name: io.netty:netty-bom\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n- dependency-name: io.netty:netty-codec-http\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n- dependency-name: io.netty:netty-codec-haproxy\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2023-05-01T04:23:28Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/a48c56f4e2e70371222720b74ec20481e6e87a6e"
        },
        "date": 1682915693194,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 20.92,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 97,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 511,
            "unit": "ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "fb3794b146c0f2004ce4a6653b1969c9c1e14e26",
          "message": "Bump netty.version from 4.1.92.Final to 4.1.93.Final\n\nBumps `netty.version` from 4.1.92.Final to 4.1.93.Final.\n\nUpdates `netty-bom` from 4.1.92.Final to 4.1.93.Final\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.92.Final...netty-4.1.93.Final)\n\nUpdates `netty-codec-http` from 4.1.92.Final to 4.1.93.Final\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.92.Final...netty-4.1.93.Final)\n\nUpdates `netty-codec-haproxy` from 4.1.92.Final to 4.1.93.Final\n- [Commits](https://github.com/netty/netty/compare/netty-4.1.92.Final...netty-4.1.93.Final)\n\n---\nupdated-dependencies:\n- dependency-name: io.netty:netty-bom\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n- dependency-name: io.netty:netty-codec-http\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n- dependency-name: io.netty:netty-codec-haproxy\n  dependency-type: direct:production\n  update-type: version-update:semver-patch\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>",
          "timestamp": "2023-05-29T04:03:26Z",
          "url": "https://github.com/kroxylicious/kroxylicious/commit/fb3794b146c0f2004ce4a6653b1969c9c1e14e26"
        },
        "date": 1685395723620,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "AVG Latency",
            "value": 37.44,
            "unit": "ms"
          },
          {
            "name": "95th Latency",
            "value": 274,
            "unit": "ms"
          },
          {
            "name": "99th Latency",
            "value": 391,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}