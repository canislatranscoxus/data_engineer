

Intro

In this folder we have some scripts that run locally in laptop, no need to use cloud.
We will start with the most simple and basic ang the next script will add more complexity,
the idea is gradually add and verify they work.

Batch pipelines


| num | bound | branches | source | target   |
|-----|-------|----------|--------|----------|
| 1   | batch | 1        | file   | file     |
| 2   | batch | 2        | file   | file     |
| 3   | batch | 2        | file   | postgres |

Stream pipelines

| num | bound | branches | source | target    |
|-----|--------|----------|--------|----------|
| 4   | stream | 1        | kafka  | file     |
| 5   | stream | 2        | kafka  | file     |
| 6   | stream | 2        | kafka  | postgres |

