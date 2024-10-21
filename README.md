# nfl_dbt_project
This repository contains a DAG that runs every 4 days to upload pbp from the current nfl season (2024) and uploads it to a data lake in Snowflake and is used by dbt to create models.
## Models
`games`: lists all the games in the season with season type (`PRE`, `REG`, `POST`).

`plays`: list all plays by games.

`epa`: list the epa of each play.

## Workflow

![diagram.png](https://github.com/NisoomV/nfl_dbt_project/blob/91705de1692ca647f6237d0651c57e2060cb5c17/diagram.png)

## Tools Used
**Languages:**  Python, SQL

**Transformations:**  PySpark, dbt

**Storage:**  Snowflake

**Other Tools:**  draw.io
