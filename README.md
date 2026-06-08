# graph-query-benchmark

Skrypt benchmarkowy do porównania zapytań SPARQL, Cypher oraz GQL dla danych LDBC SNB.

W projekcie porównywane są trzy warianty zapytań:

- SPARQL wykonywany w Apache Jena Fuseki,
- Cypher wykonywany w Neo4j,
- GQL wykonywany w Neo4j.

Projekt powstał na potrzeby pracy magisterskiej dotyczącej analizy porównawczej języków zapytań dla grafów własności oraz RDF.

## Wymagania

- Python 3.13,
- Apache Jena Fuseki,
- Neo4j,
- biblioteki Python wymienione w pliku `requirements.txt`.

## Instalacja zależności

```bash
pip install -r requirements.txt
````

## Konfiguracja

Do lokalnego uruchamiania benchmarku należy utworzyć własny plik:

```text
benchmark_config.json
```

Hasło do Neo4j można podać bezpośrednio w pliku `benchmark_config.json` albo przez zmienną środowiskową w terminalu:

```powershell
$env:NEO4J_PASSWORD="twoje_haslo"
```

W konfiguracji dla każdego zapytania należy wskazać trzy pliki:

```json
{
  "name": "Q01",
  "sparql_file": "queries/sparql/Q01.rq",
  "cypher_file": "queries/cypher/Q01.cypher",
  "gql_file": "queries/gql/Q01.gql"
}
```

## Tryby uruchomienia

### Krótki test techniczny

```bash
python benchmark_graph_queries.py --config benchmark_config.json --output results/sf01_test.csv --cache-mode warm --engines all --warmups 2 --repeats 3
```

### Warm cache

Tryb warm cache wykonuje najpierw uruchomienia rozgrzewkowe, które nie są uwzględniane w wynikach końcowych, a następnie wykonuje właściwe pomiary.

```bash
python benchmark_graph_queries.py --config benchmark_config.json --output results/sf01_warm.csv --cache-mode warm --engines all --warmups 10 --repeats 30
```

### Cold cache

Tryb cold cache wymaga skonfigurowania komend resetujących środowisko. Komendy można ustawić globalnie w sekcji `settings` albo osobno dla danego scale factora.

```bash
python benchmark_graph_queries.py --config benchmark_config.json --output results/sf01_cold.csv --cache-mode cold --engines all --repeats 5
```

Jeżeli komendy resetujące nie są ustawione, skrypt wypisze ostrzeżenie, ponieważ nie można wtedy zagwarantować prawdziwego cold cache.

### Wybór wariantów zapytań

Dostępne wartości parametru `--engines`:

```text
fuseki  - tylko SPARQL/Fuseki
neo4j   - tylko Cypher/Neo4j
gql     - tylko GQL/Neo4j
both    - SPARQL/Fuseki oraz Cypher/Neo4j
all     - SPARQL/Fuseki, Cypher/Neo4j oraz GQL/Neo4j
```

## Walidacja wyników

Przed właściwymi pomiarami skrypt wykonuje osobny etap walidacji poprawności wyników, który nie jest wliczany do czasu benchmarku. Walidacja polega na pobraniu wyników z porównywanych wariantów zapytań, normalizacji wartości oraz porównaniu zbiorów rekordów.

Dla trybu `--engines all` wykonywana jest walidacja:

```text
SPARQL vs Cypher
SPARQL vs GQL
```

Porównanie `Cypher vs GQL` nie jest wykonywane osobno, ponieważ oba warianty są porównywane z tym samym wynikiem referencyjnym SPARQL. Jeżeli `SPARQL vs Cypher` oraz `SPARQL vs GQL` przejdą walidację, oznacza to również zgodność wyników Cypher i GQL.

Walidację można pominąć opcją:

```bash
python benchmark_graph_queries.py --config benchmark_config.json --output results/sf01_warm.csv --cache-mode warm --engines all --skip-validation
```

## Pliki wynikowe

Skrypt zapisuje dwa pliki wynikowe.

Pierwszy plik zawiera podsumowanie wyników z wielu powtórzeń, np.:

```text
results/sf01_warm.csv
```

Drugi plik zawiera pojedyncze pomiary wykonania zapytań, np.:

```text
results/sf01_warm_samples.csv
```

Plik z podsumowaniem wyników zawiera m.in.:

* średnią,
* medianę,
* minimum,
* maksimum,
* odchylenie standardowe,
* percentyl 95,
* minimalną i maksymalną liczbę rekordów.

Drugi plik zawiera pojedyncze pomiary wykonania zapytań, m.in.:

* scale factor,
* wariant zapytania,
* nazwę zapytania,
* tryb cache,
* numer powtórzenia,
* czas wykonania,
* liczbę rekordów.

## Timeouty

W skrypcie osobno ustawiane są limity czasu dla komunikacji z bazą i dla wykonania zapytania:

* `http_timeout_seconds` — timeout klienta HTTP dla Fuseki,
* `query_timeout_seconds` — timeout zapytania wykonywanego w Neo4j,
* `fuseki_query_timeout_ms` — opcjonalny timeout zapytania przekazywany do Fuseki.

## Licencja

Projekt jest udostępniany na licencji MIT.
