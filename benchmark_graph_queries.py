#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shlex
import statistics
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from neo4j import GraphDatabase, Query
from neo4j.graph import Node, Relationship, Path as Neo4jPath


@dataclass(frozen=True)
class QueryPair:
    name: str
    sparql_file: Path
    cypher_file: Path
    gql_file: Path


@dataclass(frozen=True)
class ScaleFactorConfig:
    name: str
    fuseki_endpoint: str
    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str
    neo4j_database: Optional[str] = None
    fuseki_cold_reset_command: Optional[str] = None
    neo4j_cold_reset_command: Optional[str] = None


def read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Brak pliku: {path}")
    return path.read_text(encoding="utf-8")


def percentile(values: List[float], p: float) -> float:
    if not values:
        return math.nan

    ordered = sorted(values)

    if len(ordered) == 1:
        return ordered[0]

    k = (len(ordered) - 1) * (p / 100.0)
    floor = math.floor(k)
    ceil = math.ceil(k)

    if floor == ceil:
        return ordered[int(k)]

    lower = ordered[floor] * (ceil - k)
    upper = ordered[ceil] * (k - floor)

    return lower + upper


def compute_stats(times_ms: List[float]) -> Dict[str, float]:
    if not times_ms:
        raise ValueError(
            "Brak pomiarów do obliczenia statystyk. "
            "Parametr repeats musi być większy lub równy 1."
        )

    mean_ms = statistics.mean(times_ms)
    stddev_ms = statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0

    return {
        "mean_ms": mean_ms,
        "median_ms": statistics.median(times_ms),
        "stddev_ms": stddev_ms,
        "variance_ms2": statistics.variance(times_ms) if len(times_ms) > 1 else 0.0,
        "min_ms": min(times_ms),
        "max_ms": max(times_ms),
        "range_ms": max(times_ms) - min(times_ms),
        "coefficient_of_variation": stddev_ms / mean_ms if mean_ms != 0 else 0.0,
        "p25_ms": percentile(times_ms, 25),
        "p50_ms": percentile(times_ms, 50),
        "p75_ms": percentile(times_ms, 75),
        "p90_ms": percentile(times_ms, 90),
        "p95_ms": percentile(times_ms, 95),
        "p99_ms": percentile(times_ms, 99),
    }


def run_command(command: Optional[str], sleep_seconds: float = 0.0) -> None:
    if not command:
        return

    command_args = shlex.split(command)

    print(f"[reset] {' '.join(command_args)}", flush=True)

    completed = subprocess.run(command_args, shell=False)

    if completed.returncode != 0:
        raise RuntimeError(f"Komenda resetująca zakończyła się błędem: {command}")

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)


def run_sparql_query(
    session: requests.Session,
    endpoint: str,
    query: str,
    http_timeout_seconds: int,
    fuseki_query_timeout_ms: Optional[int] = None,
) -> int:
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-query; charset=utf-8",
    }

    params = {}

    if fuseki_query_timeout_ms is not None:
        params["timeout"] = str(fuseki_query_timeout_ms)

    response = session.post(
        endpoint,
        params=params,
        data=query.encode("utf-8"),
        headers=headers,
        timeout=http_timeout_seconds,
    )
    response.raise_for_status()
    # response.json() wymusza pobranie i sparsowanie całej odpowiedzi z Fuseki.
    # Dzięki temu mierzę wykonanie całego zapytania, a nie tylko rozpoczęcie odpowiedzi.
    data = response.json()

    if "results" in data and "bindings" in data["results"]:
        return len(data["results"]["bindings"])

    if "boolean" in data:
        return 1

    if isinstance(data, list):
        return len(data)

    return 0


def run_neo4j_query(
    driver: Any,
    database: Optional[str],
    query: str,
    query_timeout_seconds: int,
) -> int:
    session_kwargs: Dict[str, Any] = {}

    if database:
        session_kwargs["database"] = database

    neo4j_query = Query(query, timeout=query_timeout_seconds)

    with driver.session(**session_kwargs) as session:
        result = session.run(neo4j_query)
        # list(result) wymusza pobranie wszystkich rekordów z Neo4j.
        # Bez tego można byłoby zmierzyć tylko rozpoczęcie strumienia wyników.
        records = list(result)
        result.consume()
        return len(records)


def normalize_value(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, Node):
        props = dict(value.items())

        if "id" in props:
            return str(props["id"])

        return str(sorted((str(k), normalize_value(v)) for k, v in props.items()))

    if isinstance(value, Relationship):
        props = dict(value.items())

        if "id" in props:
            return str(props["id"])

        return str(sorted((str(k), normalize_value(v)) for k, v in props.items()))

    if isinstance(value, Neo4jPath):
        return str(value)

    if isinstance(value, (list, tuple)):
        return "[" + ",".join(normalize_value(v) for v in value) + "]"

    if isinstance(value, dict):
        return str(sorted((str(k), normalize_value(v)) for k, v in value.items()))

    text = str(value)

    if text.endswith(".0"):
        text = text[:-2]

    return text.strip()


def normalize_sparql_binding(
    binding: Dict[str, Any],
    columns: List[str],
) -> Tuple[str, ...]:
    row = []

    for col in columns:
        value_info = binding.get(col)

        if value_info is None:
            row.append("")
        else:
            row.append(normalize_value(value_info.get("value")))

    return tuple(row)


def normalize_neo4j_record(
    record: Any,
    columns: List[str],
) -> Tuple[str, ...]:
    row = []

    for col in columns:
        value = record.get(col, None)
        row.append(normalize_value(value))

    return tuple(row)


def fetch_sparql_result_multiset(
    session: requests.Session,
    endpoint: str,
    query: str,
    http_timeout_seconds: int,
    fuseki_query_timeout_ms: Optional[int] = None,
) -> Tuple[List[str], Counter[Tuple[str, ...]]]:
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-query; charset=utf-8",
    }

    params = {}

    if fuseki_query_timeout_ms is not None:
        params["timeout"] = str(fuseki_query_timeout_ms)

    response = session.post(
        endpoint,
        params=params,
        data=query.encode("utf-8"),
        headers=headers,
        timeout=http_timeout_seconds,
    )
    response.raise_for_status()

    data = response.json()

    columns = data.get("head", {}).get("vars", [])
    bindings = data.get("results", {}).get("bindings", [])

    normalized_rows = Counter(
        normalize_sparql_binding(binding, columns)
        for binding in bindings
    )

    return columns, normalized_rows


def fetch_neo4j_result_multiset(
    driver: Any,
    database: Optional[str],
    query: str,
    query_timeout_seconds: int,
) -> Tuple[List[str], Counter[Tuple[str, ...]]]:
    session_kwargs: Dict[str, Any] = {}

    if database:
        session_kwargs["database"] = database

    neo4j_query = Query(query, timeout=query_timeout_seconds)

    with driver.session(**session_kwargs) as session:
        result = session.run(neo4j_query)
        columns = list(result.keys())
        records = list(result)
        result.consume()

    normalized_rows = Counter(
        normalize_neo4j_record(record, columns)
        for record in records
    )

    return columns, normalized_rows


def validate_query_results(
    sf: ScaleFactorConfig,
    query: QueryPair,
    sparql_text: str,
    other_text: str,
    other_label: str,
    http_timeout_seconds: int,
    query_timeout_seconds: int,
    fuseki_query_timeout_ms: Optional[int],
) -> bool:
    # Walidacja jest wykonywana poza właściwym pomiarem czasu.
    # Służy sprawdzeniu, czy warianty zapytań zwracają te same rekordy
    # z taką samą liczbą wystąpień.
    print(
        f"\n[VALIDATION] SF={sf.name}, query={query.name}: "
        f"porównywanie wyników SPARQL i {other_label}",
        flush=True,
    )

    with requests.Session() as sparql_session:
        sparql_columns, sparql_rows = fetch_sparql_result_multiset(
            sparql_session,
            sf.fuseki_endpoint,
            sparql_text,
            http_timeout_seconds,
            fuseki_query_timeout_ms,
        )

    neo4j_driver = GraphDatabase.driver(
        sf.neo4j_uri,
        auth=(sf.neo4j_user, sf.neo4j_password),
    )

    try:
        neo4j_driver.verify_connectivity()

        other_columns, other_rows = fetch_neo4j_result_multiset(
            neo4j_driver,
            sf.neo4j_database,
            other_text,
            query_timeout_seconds,
        )

    finally:
        neo4j_driver.close()

    if sparql_columns != other_columns:
        print(
            f"[VALIDATION ERROR] SF={sf.name}, query={query.name}: "
            f"różne kolumny wyników SPARQL i {other_label}.",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"  SPARQL columns={sparql_columns}",
            file=sys.stderr,
            flush=True,
        )
        print(
            f"  {other_label} columns={other_columns}",
            file=sys.stderr,
            flush=True,
        )
        return False

    if sparql_rows == other_rows:
        print(
            f"[VALIDATION OK] SF={sf.name}, query={query.name}, "
            f"SPARQL vs {other_label}, records={sparql_rows.total()}",
            flush=True,
        )
        return True

    only_sparql = sparql_rows - other_rows
    only_other = other_rows - sparql_rows

    print(
        f"[VALIDATION ERROR] SF={sf.name}, query={query.name}: "
        f"wyniki SPARQL i {other_label} różnią się.",
        file=sys.stderr,
        flush=True,
    )

    print(
        f"  SPARQL records={sparql_rows.total()}, {other_label} records={other_rows.total()}",
        file=sys.stderr,
        flush=True,
    )

    if only_sparql:
        row, count = next(iter(only_sparql.items()))
        print(
            f"  Przykład rekordu tylko w SPARQL: {row}, liczba dodatkowych wystąpień={count}",
            file=sys.stderr,
            flush=True,
        )

    if only_other:
        row, count = next(iter(only_other.items()))
        print(
            f"  Przykład rekordu tylko w {other_label}: {row}, liczba dodatkowych wystąpień={count}",
            file=sys.stderr,
            flush=True,
        )

    return False


def measure_single_engine(
    engine: str,
    sf: ScaleFactorConfig,
    query_name: str,
    query_text: str,
    warmups: int,
    repeats: int,
    cache_mode: str,
    http_timeout_seconds: int,
    query_timeout_seconds: int,
    fuseki_query_timeout_ms: Optional[int],
    cold_reset_command: Optional[str],
    cold_reset_sleep_seconds: float,
    samples: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if engine not in {"fuseki", "neo4j", "gql"}:
        raise ValueError(f"Nieznany silnik/wariant zapytania: {engine}")

    if cache_mode not in {"warm", "cold"}:
        raise ValueError(f"Nieznany tryb cache: {cache_mode}")

    times_ms: List[float] = []
    record_counts: List[int] = []

    # ----------------------------------------------------------------------
    # WARM CACHE
    # ----------------------------------------------------------------------
    if cache_mode == "warm":
        neo4j_driver = None
        sparql_session = None

        if engine in {"neo4j", "gql"}:
            neo4j_driver = GraphDatabase.driver(
                sf.neo4j_uri,
                auth=(sf.neo4j_user, sf.neo4j_password),
            )
            neo4j_driver.verify_connectivity()

        elif engine == "fuseki":
            sparql_session = requests.Session()

        try:
            # Uruchomienia rozgrzewkowe nie są zapisywane w wynikach,
            # ponieważ służą tylko ustabilizowaniu warm cache.
            for i in range(warmups):
                if engine == "fuseki":
                    count = run_sparql_query(
                        sparql_session,
                        sf.fuseki_endpoint,
                        query_text,
                        http_timeout_seconds,
                        fuseki_query_timeout_ms,
                    )
                else:
                    count = run_neo4j_query(
                        neo4j_driver,
                        sf.neo4j_database,
                        query_text,
                        query_timeout_seconds,
                    )

                print(
                    f"  warmup {i + 1}/{warmups}: {engine}, {query_name}, records={count}",
                    flush=True,
                )

            for i in range(repeats):
                start = time.perf_counter()

                if engine == "fuseki":
                    count = run_sparql_query(
                        sparql_session,
                        sf.fuseki_endpoint,
                        query_text,
                        http_timeout_seconds,
                        fuseki_query_timeout_ms,
                    )
                else:
                    count = run_neo4j_query(
                        neo4j_driver,
                        sf.neo4j_database,
                        query_text,
                        query_timeout_seconds,
                    )

                elapsed_ms = (time.perf_counter() - start) * 1000.0

                times_ms.append(elapsed_ms)
                record_counts.append(count)
                # Plik samples.csv przechowuje pojedyncze pomiary,
                # co pozwala później analizować rozkład wyników i wartości odstające.
                samples.append({
                    "scale_factor": sf.name,
                    "engine": engine,
                    "query": query_name,
                    "cache_mode": cache_mode,
                    "run_index": i + 1,
                    "elapsed_ms": elapsed_ms,
                    "record_count": count,
                })

                print(
                    f"  run {i + 1}/{repeats}: {engine}, {query_name}, "
                    f"{elapsed_ms:.3f} ms, records={count}",
                    flush=True,
                )

        finally:
            if neo4j_driver is not None:
                neo4j_driver.close()

            if sparql_session is not None:
                sparql_session.close()

    # ----------------------------------------------------------------------
    # COLD CACHE
    # ----------------------------------------------------------------------
    else:
        for i in range(repeats):
            # W trybie cold reset wykonuje przed utworzeniem sesji,
            # aby nie używać starych połączeń po restarcie bazy.
            run_command(cold_reset_command, sleep_seconds=cold_reset_sleep_seconds)

            if engine in {"neo4j", "gql"}:
                neo4j_driver = GraphDatabase.driver(
                    sf.neo4j_uri,
                    auth=(sf.neo4j_user, sf.neo4j_password),
                )

                try:
                    neo4j_driver.verify_connectivity()

                    start = time.perf_counter()

                    count = run_neo4j_query(
                        neo4j_driver,
                        sf.neo4j_database,
                        query_text,
                        query_timeout_seconds,
                    )

                    elapsed_ms = (time.perf_counter() - start) * 1000.0

                finally:
                    neo4j_driver.close()

            else:
                sparql_session = requests.Session()

                try:
                    start = time.perf_counter()

                    count = run_sparql_query(
                        sparql_session,
                        sf.fuseki_endpoint,
                        query_text,
                        http_timeout_seconds,
                        fuseki_query_timeout_ms,
                    )

                    elapsed_ms = (time.perf_counter() - start) * 1000.0

                finally:
                    sparql_session.close()

            times_ms.append(elapsed_ms)
            record_counts.append(count)
            # Plik samples.csv przechowuje pojedyncze pomiary,
            # co pozwala później analizować rozkład wyników i wartości odstające.
            samples.append({
                "scale_factor": sf.name,
                "engine": engine,
                "query": query_name,
                "cache_mode": cache_mode,
                "run_index": i + 1,
                "elapsed_ms": elapsed_ms,
                "record_count": count,
            })

            print(
                f"  run {i + 1}/{repeats}: {engine}, {query_name}, "
                f"{elapsed_ms:.3f} ms, records={count}",
                flush=True,
            )

    stats = compute_stats(times_ms)

    record_count_min = min(record_counts) if record_counts else 0
    record_count_max = max(record_counts) if record_counts else 0
    record_count_stable = record_count_min == record_count_max

    return {
        "scale_factor": sf.name,
        "engine": engine,
        "query": query_name,
        "cache_mode": cache_mode,
        "warmups": warmups if cache_mode == "warm" else 0,
        "repeats": repeats,
        "record_count_min": record_count_min,
        "record_count_max": record_count_max,
        "record_count_stable": record_count_stable,
        **stats,
    }


def load_config(path: Path) -> Tuple[List[ScaleFactorConfig], List[QueryPair], Dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    base_dir = path.parent

    settings = raw.get("settings", {})

    scale_factors = [
        ScaleFactorConfig(
            name=str(item["name"]),
            fuseki_endpoint=item["fuseki_endpoint"],
            neo4j_uri=item["neo4j_uri"],
            neo4j_user=item["neo4j_user"],
            neo4j_password=item.get("neo4j_password") or os.getenv("NEO4J_PASSWORD", ""),
            neo4j_database=item.get("neo4j_database"),
            fuseki_cold_reset_command=item.get("fuseki_cold_reset_command"),
            neo4j_cold_reset_command=item.get("neo4j_cold_reset_command"),
        )
        for item in raw["scale_factors"]
    ]

    queries = [
        QueryPair(
            name=item["name"],
            sparql_file=(base_dir / item["sparql_file"]).resolve(),
            cypher_file=(base_dir / item["cypher_file"]).resolve(),
            gql_file=(base_dir / item["gql_file"]).resolve(),
        )
        for item in raw["queries"]
    ]

    return scale_factors, queries, settings


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("Brak wyników do zapisania.")

    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "scale_factor",
        "engine",
        "query",
        "cache_mode",
        "warmups",
        "repeats",
        "record_count_min",
        "record_count_max",
        "record_count_stable",
        "mean_ms",
        "median_ms",
        "stddev_ms",
        "variance_ms2",
        "min_ms",
        "max_ms",
        "range_ms",
        "coefficient_of_variation",
        "p25_ms",
        "p50_ms",
        "p75_ms",
        "p90_ms",
        "p95_ms",
        "p99_ms",
    ]

    rounded_rows = []

    for row in rows:
        formatted = row.copy()

        for key in [
            "mean_ms",
            "median_ms",
            "stddev_ms",
            "variance_ms2",
            "min_ms",
            "max_ms",
            "range_ms",
            "coefficient_of_variation",
            "p25_ms",
            "p50_ms",
            "p75_ms",
            "p90_ms",
            "p95_ms",
            "p99_ms",
        ]:
            if key in formatted and isinstance(formatted[key], (int, float)):
                formatted[key] = f"{formatted[key]:.3f}".replace(".", ",")

        rounded_rows.append(formatted)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=";",
        )
        writer.writeheader()

        for row in rounded_rows:
            writer.writerow(row)


def write_samples_csv(path: Path, samples: List[Dict[str, Any]]) -> None:
    if not samples:
        raise ValueError("Brak próbek do zapisania.")

    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "scale_factor",
        "engine",
        "query",
        "cache_mode",
        "run_index",
        "elapsed_ms",
        "record_count",
    ]

    formatted_samples = []

    for sample in samples:
        formatted = sample.copy()

        if "elapsed_ms" in formatted and isinstance(formatted["elapsed_ms"], (int, float)):
            formatted["elapsed_ms"] = f"{formatted['elapsed_ms']:.3f}".replace(".", ",")

        formatted_samples.append(formatted)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=";",
        )
        writer.writeheader()

        for sample in formatted_samples:
            writer.writerow(sample)


def compare_record_counts(rows: List[Dict[str, Any]]) -> None:
    grouped: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for row in rows:
        key = (row["scale_factor"], row["query"], row["cache_mode"])
        grouped.setdefault(key, {})[row["engine"]] = row

    for (sf, query, cache_mode), engines in grouped.items():
        if "fuseki" not in engines:
            continue

        fuseki_row = engines["fuseki"]
        fuseki_range = (
            fuseki_row["record_count_min"],
            fuseki_row["record_count_max"],
        )

        if not fuseki_row["record_count_stable"]:
            print(
                f"[UWAGA] Niestabilna liczba rekordów w Fuseki: "
                f"SF={sf}, query={query}, cache={cache_mode}, zakres={fuseki_range}",
                file=sys.stderr,
                flush=True,
            )

        for other_engine in ["neo4j", "gql"]:
            if other_engine not in engines:
                continue

            other_row = engines[other_engine]
            other_range = (
                other_row["record_count_min"],
                other_row["record_count_max"],
            )

            if not other_row["record_count_stable"]:
                print(
                    f"[UWAGA] Niestabilna liczba rekordów w {other_engine}: "
                    f"SF={sf}, query={query}, cache={cache_mode}, zakres={other_range}",
                    file=sys.stderr,
                    flush=True,
                )

            if fuseki_range != other_range:
                print(
                    f"[UWAGA] Różny zakres liczby rekordów: SF={sf}, query={query}, "
                    f"cache={cache_mode}, Fuseki={fuseki_range}, "
                    f"{other_engine}={other_range}",
                    file=sys.stderr,
                    flush=True,
                )


def validate_benchmark_parameters(
    warmups: int,
    repeats: int,
    http_timeout_seconds: int,
    query_timeout_seconds: int,
    fuseki_query_timeout_ms: Optional[int],
    cold_reset_sleep_seconds: float,
) -> None:
    if warmups < 0:
        raise ValueError("Parametr warmups musi być większy lub równy 0.")

    if repeats < 1:
        raise ValueError("Parametr repeats musi być większy lub równy 1.")

    if http_timeout_seconds < 1:
        raise ValueError("Parametr http_timeout_seconds musi być większy lub równy 1.")

    if query_timeout_seconds < 1:
        raise ValueError("Parametr query_timeout_seconds musi być większy lub równy 1.")

    if fuseki_query_timeout_ms is not None and fuseki_query_timeout_ms < 1:
        raise ValueError("Parametr fuseki_query_timeout_ms musi być większy lub równy 1.")

    if cold_reset_sleep_seconds < 0:
        raise ValueError("Parametr cold_reset_sleep_seconds musi być większy lub równy 0.")


def resolve_engines(engines_arg: str) -> List[str]:
    if engines_arg == "both":
        return ["fuseki", "neo4j"]

    if engines_arg == "all":
        return ["fuseki", "neo4j", "gql"]

    return [engines_arg]


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        required=True,
        help="Ścieżka do pliku JSON z konfiguracją benchmarku.",
    )
    parser.add_argument(
        "--output",
        default="benchmark_results.csv",
        help="Plik CSV z wynikami zagregowanymi.",
    )
    parser.add_argument(
        "--cache-mode",
        choices=["warm", "cold", "both"],
        default="warm",
    )
    parser.add_argument(
        "--engines",
        choices=["both", "all", "fuseki", "neo4j", "gql"],
        default="both",
        help=(
            "Silniki/warianty zapytań: "
            "fuseki=SPARQL/Fuseki, neo4j=Cypher/Neo4j, "
            "gql=GQL/Neo4j, both=Fuseki+Cypher, all=Fuseki+Cypher+GQL."
        ),
    )
    parser.add_argument("--warmups", type=int, default=None)
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--http-timeout-seconds", type=int, default=None)
    parser.add_argument("--query-timeout-seconds", type=int, default=None)
    parser.add_argument("--fuseki-query-timeout-ms", type=int, default=None)
    parser.add_argument("--cold-reset-sleep-seconds", type=float, default=None)
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Pomiń walidację.",
    )

    args = parser.parse_args()

    scale_factors, queries, settings = load_config(Path(args.config))

    warmups = (
        args.warmups
        if args.warmups is not None
        else int(settings.get("warmups", 10))
    )

    repeats = (
        args.repeats
        if args.repeats is not None
        else int(settings.get("repeats", 30))
    )

    http_timeout_seconds = (
        args.http_timeout_seconds
        if args.http_timeout_seconds is not None
        else int(settings.get("http_timeout_seconds", 300))
    )

    query_timeout_seconds = (
        args.query_timeout_seconds
        if args.query_timeout_seconds is not None
        else int(settings.get("query_timeout_seconds", 300))
    )

    fuseki_query_timeout_ms = (
        args.fuseki_query_timeout_ms
        if args.fuseki_query_timeout_ms is not None
        else settings.get("fuseki_query_timeout_ms")
    )

    if fuseki_query_timeout_ms is not None:
        fuseki_query_timeout_ms = int(fuseki_query_timeout_ms)

    cold_reset_sleep_seconds = (
        args.cold_reset_sleep_seconds
        if args.cold_reset_sleep_seconds is not None
        else float(settings.get("cold_reset_sleep_seconds", 10))
    )

    validate_benchmark_parameters(
        warmups=warmups,
        repeats=repeats,
        http_timeout_seconds=http_timeout_seconds,
        query_timeout_seconds=query_timeout_seconds,
        fuseki_query_timeout_ms=fuseki_query_timeout_ms,
        cold_reset_sleep_seconds=cold_reset_sleep_seconds,
    )

    cache_modes = ["warm", "cold"] if args.cache_mode == "both" else [args.cache_mode]
    engines = resolve_engines(args.engines)

    rows: List[Dict[str, Any]] = []
    samples: List[Dict[str, Any]] = []

    for sf in scale_factors:
        for query in queries:
            sparql_text = read_text(query.sparql_file)
            cypher_text = read_text(query.cypher_file)
            gql_text = read_text(query.gql_file)

            if not args.skip_validation:
                if "neo4j" in engines:
                    validation_ok = validate_query_results(
                        sf=sf,
                        query=query,
                        sparql_text=sparql_text,
                        other_text=cypher_text,
                        other_label="Cypher",
                        http_timeout_seconds=http_timeout_seconds,
                        query_timeout_seconds=query_timeout_seconds,
                        fuseki_query_timeout_ms=fuseki_query_timeout_ms,
                    )

                    if not validation_ok:
                        raise RuntimeError(
                            f"Walidacja wyników nie powiodła się: "
                            f"SF={sf.name}, query={query.name}, wariant=Cypher. "
                            "Popraw zapytania przed wykonaniem benchmarku."
                        )

                if "gql" in engines:
                    validation_ok = validate_query_results(
                        sf=sf,
                        query=query,
                        sparql_text=sparql_text,
                        other_text=gql_text,
                        other_label="GQL",
                        http_timeout_seconds=http_timeout_seconds,
                        query_timeout_seconds=query_timeout_seconds,
                        fuseki_query_timeout_ms=fuseki_query_timeout_ms,
                    )

                    if not validation_ok:
                        raise RuntimeError(
                            f"Walidacja wyników nie powiodła się: "
                            f"SF={sf.name}, query={query.name}, wariant=GQL. "
                            "Popraw zapytania przed wykonaniem benchmarku."
                        )

            for cache_mode in cache_modes:
                for engine in engines:
                    print(
                        f"\n=== SF={sf.name}, engine={engine}, "
                        f"query={query.name}, cache={cache_mode} ===",
                        flush=True,
                    )

                    if engine == "fuseki":
                        query_text = sparql_text
                    elif engine == "neo4j":
                        query_text = cypher_text
                    elif engine == "gql":
                        query_text = gql_text
                    else:
                        raise ValueError(f"Nieznany silnik/wariant zapytania: {engine}")

                    cold_reset_command = None

                    if cache_mode == "cold":
                        if engine == "fuseki":
                            cold_reset_command = (
                                sf.fuseki_cold_reset_command
                                or settings.get("fuseki_cold_reset_command")
                            )
                        else:
                            cold_reset_command = (
                                sf.neo4j_cold_reset_command
                                or settings.get("neo4j_cold_reset_command")
                            )

                        if not cold_reset_command:
                            raise RuntimeError(
                                f"Tryb cold wymaga komendy resetującej cache. "
                                f"Brak komendy dla SF={sf.name}, engine={engine}, query={query.name}."
                            )

                    row = measure_single_engine(
                        engine=engine,
                        sf=sf,
                        query_name=query.name,
                        query_text=query_text,
                        warmups=warmups,
                        repeats=repeats,
                        cache_mode=cache_mode,
                        http_timeout_seconds=http_timeout_seconds,
                        query_timeout_seconds=query_timeout_seconds,
                        fuseki_query_timeout_ms=fuseki_query_timeout_ms,
                        cold_reset_command=cold_reset_command,
                        cold_reset_sleep_seconds=cold_reset_sleep_seconds,
                        samples=samples,
                    )

                    rows.append(row)

    compare_record_counts(rows)

    output_path = Path(args.output)
    samples_path = output_path.with_name(output_path.stem + "_samples.csv")

    write_csv(output_path, rows)
    write_samples_csv(samples_path, samples)

    print(f"\nZapisano wyniki do: {output_path}", flush=True)
    print(f"Zapisano próbki do: {samples_path}", flush=True)


if __name__ == "__main__":
    main()
