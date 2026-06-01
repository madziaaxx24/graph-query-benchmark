#!/usr/bin/env python3
"""
Uruchomienie:
    python benchmark_graph_queries.py --config benchmark_config.json --output results.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from neo4j import GraphDatabase, Query


@dataclass(frozen=True)
class QueryPair:
    name: str
    sparql_file: Path
    cypher_file: Path


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
    """
    Percentyl liczony metodą interpolacji liniowej.
    Dla p=95 zwraca p95.
    """
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

    return {
        "mean_ms": statistics.mean(times_ms),
        "median_ms": statistics.median(times_ms),
        "min_ms": min(times_ms),
        "max_ms": max(times_ms),
        "stddev_ms": statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0,
        "p95_ms": percentile(times_ms, 95),
    }


def run_command(command: Optional[str], sleep_seconds: float = 0.0) -> None:
    """
    Opcjonalna komenda używana np. do restartu kontenera/bazy przed pomiarem cold cache.
    Przykłady:
        docker restart fuseki_sf01
        docker restart neo4j_sf01
    """
    if not command:
        return

    print(f"[reset] {command}", flush=True)
    completed = subprocess.run(command, shell=True)
    if completed.returncode != 0:
        raise RuntimeError(f"Komenda resetująca zakończyła się błędem: {command}")

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)


def run_sparql_query(endpoint: str, query: str, timeout_seconds: int) -> int:
    """
    Wykonuje zapytanie SPARQL przez HTTP i wymusza pobranie całej odpowiedzi.
    Zwraca liczbę rekordów/wyników.

    Dla SELECT liczy bindings.
    Dla ASK liczy 1 rekord logiczny.
    Dla CONSTRUCT/DESCRIBE przy domyślnym JSON może to wymagać dostosowania.
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-query; charset=utf-8",
    }

    response = requests.post(
        endpoint,
        data=query.encode("utf-8"),
        headers=headers,
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    # response.json() wymusza pobranie i sparsowanie całego wyniku.
    data = response.json()

    if "results" in data and "bindings" in data["results"]:
        return len(data["results"]["bindings"])

    if "boolean" in data:
        return 1

    # Awaryjnie: jeśli endpoint zwrócił inny format JSON.
    if isinstance(data, list):
        return len(data)

    return 0


def run_cypher_query(
    driver: Any,
    database: Optional[str],
    query: str,
    timeout_seconds: int,
) -> int:
    """
    Wykonuje zapytanie Cypher i wymusza pobranie całego wyniku przez list(result).
    Zwraca liczbę rekordów.
    """
    session_kwargs: Dict[str, Any] = {}
    if database:
        session_kwargs["database"] = database

    cypher_query = Query(query, timeout=timeout_seconds)

    with driver.session(**session_kwargs) as session:
        result = session.run(cypher_query)
        records = list(result)  # wymuszenie pobrania całego wyniku
        result.consume()        # pobranie podsumowania wykonania
        return len(records)


def measure_single_engine(
    engine: str,
    sf: ScaleFactorConfig,
    query_name: str,
    query_text: str,
    warmups: int,
    repeats: int,
    cache_mode: str,
    timeout_seconds: int,
    cold_reset_command: Optional[str],
    cold_reset_sleep_seconds: float,
) -> Dict[str, Any]:
    """
    Mierzy jedno zapytanie dla jednego silnika i jednego scale factora.
    """
    times_ms: List[float] = []
    record_counts: List[int] = []

    neo4j_driver = None
    if engine == "neo4j":
        neo4j_driver = GraphDatabase.driver(
            sf.neo4j_uri,
            auth=(sf.neo4j_user, sf.neo4j_password),
        )

    try:
        if cache_mode == "warm":
            # Uruchomienia rozgrzewkowe nie są uwzględniane w wynikach.
            for i in range(warmups):
                if engine == "fuseki":
                    count = run_sparql_query(sf.fuseki_endpoint, query_text, timeout_seconds)
                else:
                    count = run_cypher_query(neo4j_driver, sf.neo4j_database, query_text, timeout_seconds)
                print(f"  warmup {i + 1}/{warmups}: {engine}, {query_name}, records={count}", flush=True)

        for i in range(repeats):
            if cache_mode == "cold":
                # Prawdziwy cold cache wymaga restartu/wyczyszczenia cache poza samym zapytaniem.
                # Dlatego skrypt pozwala podać komendę resetującą środowisko.
                run_command(cold_reset_command, sleep_seconds=cold_reset_sleep_seconds)

            start = time.perf_counter()

            if engine == "fuseki":
                count = run_sparql_query(sf.fuseki_endpoint, query_text, timeout_seconds)
            else:
                count = run_cypher_query(neo4j_driver, sf.neo4j_database, query_text, timeout_seconds)

            elapsed_ms = (time.perf_counter() - start) * 1000.0

            times_ms.append(elapsed_ms)
            record_counts.append(count)

            print(
                f"  run {i + 1}/{repeats}: {engine}, {query_name}, "
                f"{elapsed_ms:.3f} ms, records={count}",
                flush=True,
            )

    finally:
        if neo4j_driver is not None:
            neo4j_driver.close()

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
            neo4j_password=item.get("neo4j_password", os.getenv("NEO4J_PASSWORD", "")),
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
        )
        for item in raw["queries"]
    ]

    return scale_factors, queries, settings


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("Brak wyników do zapisania.")

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
        "min_ms",
        "max_ms",
        "stddev_ms",
        "p95_ms",
    ]

    rounded_rows = []
    for row in rows:
        formatted = row.copy()

        for key in ["mean_ms", "median_ms", "min_ms", "max_ms", "stddev_ms", "p95_ms"]:
            if key in formatted and isinstance(formatted[key], (int, float)):
                formatted[key] = f"{formatted[key]:.3f}".replace(".", ",")

        rounded_rows.append(formatted)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            delimiter=";"
        )
        writer.writeheader()
        for row in rounded_rows:
            writer.writerow(row)



def compare_record_counts(rows: List[Dict[str, Any]]) -> None:
    """
    Kontrola poprawności porównania wyników.

    Dla tego samego scale factora, zapytania i trybu cache sprawdzamy:
    1. czy oba silniki zwracały stabilną liczbę rekordów,
    2. czy zakres liczby rekordów min..max jest taki sam w Fuseki i Neo4j.

    Samo porównanie record_count_min nie wystarcza, bo może ukryć przypadek:
    Fuseki: 10..20 rekordów, Neo4j: 10..10 rekordów.
    """
    grouped: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for row in rows:
        key = (row["scale_factor"], row["query"], row["cache_mode"])
        grouped.setdefault(key, {})[row["engine"]] = row

    for (sf, query, cache_mode), engines in grouped.items():
        if "fuseki" not in engines or "neo4j" not in engines:
            continue

        f_row = engines["fuseki"]
        n_row = engines["neo4j"]

        f_range = (f_row["record_count_min"], f_row["record_count_max"])
        n_range = (n_row["record_count_min"], n_row["record_count_max"])

        if not f_row["record_count_stable"]:
            print(
                f"[UWAGA] Niestabilna liczba rekordów w Fuseki: "
                f"SF={sf}, query={query}, cache={cache_mode}, zakres={f_range}",
                file=sys.stderr,
                flush=True,
            )

        if not n_row["record_count_stable"]:
            print(
                f"[UWAGA] Niestabilna liczba rekordów w Neo4j: "
                f"SF={sf}, query={query}, cache={cache_mode}, zakres={n_range}",
                file=sys.stderr,
                flush=True,
            )

        if f_range != n_range:
            print(
                f"[UWAGA] Różny zakres liczby rekordów: SF={sf}, query={query}, "
                f"cache={cache_mode}, Fuseki={f_range}, Neo4j={n_range}",
                file=sys.stderr,
                flush=True,
            )


def validate_benchmark_parameters(
    warmups: int,
    repeats: int,
    timeout_seconds: int,
    cold_reset_sleep_seconds: float,
) -> None:
    if warmups < 0:
        raise ValueError("Parametr warmups musi być większy lub równy 0.")

    if repeats < 1:
        raise ValueError("Parametr repeats musi być większy lub równy 1.")

    if timeout_seconds < 1:
        raise ValueError("Parametr timeout_seconds musi być większy lub równy 1.")

    if cold_reset_sleep_seconds < 0:
        raise ValueError("Parametr cold_reset_sleep_seconds musi być większy lub równy 0.")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Ścieżka do pliku JSON z konfiguracją benchmarku.")
    parser.add_argument("--output", default="benchmark_results.csv", help="Plik CSV z wynikami.")
    parser.add_argument("--cache-mode", choices=["warm", "cold", "both"], default="warm")
    parser.add_argument("--engines", choices=["both", "fuseki", "neo4j"], default="both")
    parser.add_argument("--warmups", type=int, default=None)
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--timeout-seconds", type=int, default=None)
    parser.add_argument("--cold-reset-sleep-seconds", type=float, default=None)
    args = parser.parse_args()

    scale_factors, queries, settings = load_config(Path(args.config))

    warmups = args.warmups if args.warmups is not None else int(settings.get("warmups", 10))
    repeats = args.repeats if args.repeats is not None else int(settings.get("repeats", 30))
    timeout_seconds = (
        args.timeout_seconds
        if args.timeout_seconds is not None
        else int(settings.get("timeout_seconds", 300))
    )
    cold_reset_sleep_seconds = (
        args.cold_reset_sleep_seconds
        if args.cold_reset_sleep_seconds is not None
        else float(settings.get("cold_reset_sleep_seconds", 10))
    )

    validate_benchmark_parameters(
        warmups=warmups,
        repeats=repeats,
        timeout_seconds=timeout_seconds,
        cold_reset_sleep_seconds=cold_reset_sleep_seconds,
    )

    cache_modes = ["warm", "cold"] if args.cache_mode == "both" else [args.cache_mode]
    engines = ["fuseki", "neo4j"] if args.engines == "both" else [args.engines]

    rows: List[Dict[str, Any]] = []

    for sf in scale_factors:
        for query in queries:
            sparql_text = read_text(query.sparql_file)
            cypher_text = read_text(query.cypher_file)

            for cache_mode in cache_modes:
                for engine in engines:
                    print(
                        f"\n=== SF={sf.name}, engine={engine}, query={query.name}, cache={cache_mode} ===",
                        flush=True,
                    )

                    query_text = sparql_text if engine == "fuseki" else cypher_text


                    cold_reset_command = None
                    if cache_mode == "cold":
                        per_sf_key = f"{engine}_cold_reset_command"
                        cold_reset_command = getattr(sf, per_sf_key, None)
                        cold_reset_command = cold_reset_command or settings.get(per_sf_key)

                        if not cold_reset_command:
                            print(
                                "[UWAGA] Tryb cold bez komendy resetującej. "
                                "To nie gwarantuje prawdziwego cold cache.",
                                file=sys.stderr,
                                flush=True,
                            )

                    row = measure_single_engine(
                        engine=engine,
                        sf=sf,
                        query_name=query.name,
                        query_text=query_text,
                        warmups=warmups,
                        repeats=repeats,
                        cache_mode=cache_mode,
                        timeout_seconds=timeout_seconds,
                        cold_reset_command=cold_reset_command,
                        cold_reset_sleep_seconds=cold_reset_sleep_seconds,
                    )
                    rows.append(row)


                    write_csv(Path(args.output), rows)

    compare_record_counts(rows)
    write_csv(Path(args.output), rows)
    print(f"\nZapisano wyniki do: {args.output}", flush=True)


if __name__ == "__main__":
    main()
