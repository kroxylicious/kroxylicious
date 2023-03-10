window.BENCHMARK_DATA = {
  "lastUpdate": 1678409957381,
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
      }
    ]
  }
}