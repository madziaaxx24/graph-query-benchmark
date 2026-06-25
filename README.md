# Graph Query Benchmark

This repository contains benchmark scripts and query implementations used to compare three graph query languages: **SPARQL**, **Cypher**, and **GQL**.

The benchmark was prepared for experiments based on the **LDBC Social Network Benchmark (LDBC SNB)** dataset represented in two graph models: RDF and property graph. The main goal of the project is to compare equivalent graph queries written in different query languages and measure their execution time for different dataset scale factors and cache modes.

## Repository contents

## Repository contents

- `benchmark_graph_queries.py` — main benchmark script.
- `benchmark_config.json` — benchmark configuration file.
- `requirements.txt` — Python dependencies.
- `queries/sparql/` — SPARQL query implementations for Apache Jena Fuseki.
- `queries/cypher/` — Cypher query implementations for Neo4j.
- `queries/gql/` — GQL query implementations for Neo4j.
- `scripts/reset_fuseki.ps1` — helper script for restarting Apache Jena Fuseki during cold-cache measurements.
- `scripts/reset_neo4j.ps1` — helper script for restarting Neo4j during cold-cache measurements.
- `LICENSE` — project license.
- `README.md` — repository documentation.

## Tested technologies

The benchmark was prepared and tested with the following tools:

* Python 3.13
* Apache Jena Fuseki 6.0.0 with TDB2
* Neo4j 2026.04.0
* Neo4j Desktop 2.1.4
* Neo4j Python Driver 6.2.0
* requests >= 2.31.0

## Dataset

The benchmark was prepared using the **LDBC Social Network Benchmark (LDBC SNB)** dataset.

The dataset is not included in this repository due to its size. Before running the benchmark, the LDBC SNB data should be downloaded locally and then loaded into the appropriate environments:

* an RDF store exposed through Apache Jena Fuseki for SPARQL queries,
* a Neo4j database for Cypher and GQL queries.

The experiments used locally prepared data for the following scale factors:

* SF0.1
* SF0.3
* SF1
* SF3

## Query set

The benchmark contains ten equivalent queries implemented in SPARQL, Cypher, and GQL.

The queries cover different graph query patterns, including:

* simple node retrieval,
* counting objects,
* filtering,
* negation,
* grouping and aggregation,
* sorting,
* use of `UNION`,
* multi-hop graph traversal.

## Installation

Clone the repository:

```bash
git clone https://github.com/madziaaxx24/graph-query-benchmark.git
cd graph-query-benchmark
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Before running the benchmark, edit the `benchmark_config.json` file and adjust it to your local environment.

The configuration should include:

* Apache Jena Fuseki endpoint,
* Neo4j connection URI,
* Neo4j username and password,
* paths to query directories,
* optional reset scripts used for cold-cache measurements.

Example environment variable for the Neo4j password:

```powershell
$env:NEO4J_PASSWORD="your_password"
```

## Warm-cache benchmark

In warm-cache mode, each query is executed several times before the actual measurements. These warm-up runs are not included in the final statistics.

Example command:

```bash
python benchmark_graph_queries.py --config benchmark_config.json --output results/sf01_warm.csv --cache-mode warm --engines all --warmups 10 --repeats 30
```

In this configuration:

* `--warmups 10` means that 10 warm-up runs are executed,
* `--repeats 30` means that 30 measured runs are executed,
* `--engines all` runs SPARQL, Cypher, and GQL queries.

## Cold-cache benchmark

In cold-cache mode, the database system is restarted before each measurement. No warm-up runs are used in this mode.

Example command:

```bash
python benchmark_graph_queries.py --config benchmark_config.json --output results/sf01_cold.csv --cache-mode cold --engines all --warmups 0 --repeats 30
```

The helper scripts in the `scripts/` directory can be used to restart Apache Jena Fuseki and Neo4j.

Before using them, set the required environment variables:

```powershell
$env:FUSEKI_DIR="C:\path\to\apache-jena-fuseki-6.0.0"
$env:NEO4J_BAT="C:\path\to\neo4j\bin\neo4j.bat"
```

Then the benchmark script can call:

```text
scripts/reset_fuseki.ps1
scripts/reset_neo4j.ps1
```

depending on the selected engine.

In this project, cold cache means restarting the tested database system before each measurement. The operating system page cache is not explicitly cleared.

## Output

The benchmark produces CSV files with execution statistics.

The output includes:

* mean execution time,
* median execution time,
* standard deviation,
* variance,
* minimum and maximum execution time,
* range,
* coefficient of variation,
* percentiles,
* number of returned records,
* information whether the record count was stable.

Execution time includes both query execution and fetching the full result set.

## Result validation

Before collecting benchmark measurements, the script can validate whether SPARQL, Cypher, and GQL return equivalent results.

The validation compares:

* result columns,
* normalized result values,
* multisets of returned records.

This means that duplicate rows are also taken into account.

## License

This project is licensed under the MIT License.
