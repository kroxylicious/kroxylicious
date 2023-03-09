window.BENCHMARK_DATA = {
  "lastUpdate": 1678331776961,
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
      }
    ]
  }
}