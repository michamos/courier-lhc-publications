import json
import re

import requests

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from typing import (
    Any,
    Iterable,
    Iterator,
    List,
    Dict,
    Optional,
    Set,
    Sized,
    Tuple,
    Union,
)
from urllib.parse import urljoin

from backoff import expo, on_exception

from inspire_utils.record import get_value, get_values_for_schema


INSPIRE_LITERATURE_API_ENDPOINT = "https://inspirehep.net/api/literature"
INSPIRE_FACET_API_ENDPOINT = "https://inspirehep.net/api/literature/facets"

YEARS = range(2008, 2021)

session = requests.Session()
session.headers[
    "user-agent"
] += " Courier LHC publications (mailto:micha.moshe.moskovic@cern.ch)"


@on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
def perform_inspire_literature_search(
    query: str, facets: Optional[Dict[str, str]] = None
) -> Iterator[dict]:
    """Perform the search query on INSPIRE.

    Yields:
        the json response for every record.
    """
    facets = facets or {}
    response = session.get(
        INSPIRE_LITERATURE_API_ENDPOINT, params={"q": query, **facets}
    )
    response.raise_for_status()
    content = response.json()

    for result in content["hits"]["hits"]:
        yield result

    while "next" in content.get("links", {}):
        response = session.get(content["links"]["next"])
        response.raise_for_status()
        content = response.json()

        for result in content["hits"]["hits"]:
            yield result


@on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
def perform_inspire_literature_aggregation(
    query: str, facets: Optional[Dict[str, str]] = None
) -> dict:
    """Get aggregations for the query on INSPIRE.

    Yields:
        the json response for every record.
    """
    facets = facets or {}
    response = session.get(INSPIRE_FACET_API_ENDPOINT, params={"q": query, **facets})
    response.raise_for_status()
    content = response.json()

    return content["aggregations"]


@on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
def count_inspire_literature_search(
    query: str, facets: Optional[Dict[str, str]] = None
) -> int:
    """Count results for the search query on INSPIRE.

    Returns:
        the number of matching records.
    """
    facets = facets or {}
    response = session.get(
        INSPIRE_LITERATURE_API_ENDPOINT, params={"q": query, "size": 1, **facets}
    )
    response.raise_for_status()
    content = response.json()

    return content["hits"]["total"]


def get_citations(query):
    years = iter(YEARS)
    first_year = next(years)
    for last_year in years:
        pass
    result = perform_inspire_literature_aggregation(
        query,
        facets={
            "earliest_date": f"{first_year}--{last_year}",
            "facet_name": "citation-summary",
        },
    )
    citation_stats = result["citation_summary"]["citations"]["buckets"]["all"]
    return (
        citation_stats["citations_count"]["value"],
        citation_stats["average_citations"]["value"],
    )


def annual_counts(query):
    for year in YEARS:
        hits = count_inspire_literature_search(
            query, facets={"earliest_date": f"{year}--{year}"}
        )
        yield (year, hits)


def format_annual_counts(it):
    result = ["Year|Count", "---|---"]
    for year, count in it:
        result.append(f"{year}|{count}")

    return "\n".join(result)


def get_annual_counts(query):
    counts = annual_counts(query)
    return format_annual_counts(counts)


def print_results():
    annual_queries = {
        "Total HEP": "tc core",
        "Total HEP (published)": "tc core and doi:*",
        "ALICE": "cn alice and not tc note",
        "ALICE (published)": "cn alice and tc p",
        "ATLAS": "cn atlas and not cn herschel and not tc note",
        "ATLAS (published)": "cn atlas and not cn herschel and tc p",
        "CMS": "cn cms and not tc note",
        "CMS (published)": "cn atlas and not cn herschel and tc p",
        "LHCb": "cn lhcb and not tc note",
        "LHCb (published)": "cn lhcb and not tc note and tc p",
    }

    citation_queries = {
        "Citations HEP (published)": "tc core and tc p",
        "Citations theory (published)": 'tc core and tc p and (inspire_categories.term:Theory-HEP or inspire_categories.term:Phenomenology-HEP or inspire_categories.term:"Gravitation and Cosmology" or inspire_categories.term:"Math and Math Physics" or inspire_categories.term:Theory-Nucl or inspire_categories.term:Lattice)',
        "Citations experiment (published)": "tc core and tc p and (inspire_categories.term:Experiment-HEP or inspire_categories.term:Instrumentation or inspire_categories.term:Accelerators or inspire_categories.term:Experiment-Nucl)",
        "Citations ALICE (published)": "cn alice and tc p",
        "Citations ATLAS (published)": "cn atlas and not cn herschel and tc p",
        "Citations CMS (published)": "cn cms and tc p",
        "Citations LHCb (published)": "cn lhcb and tc p",
        "Citations LHC (published)": "(cn alice or cn atlas or cn cms or cn lhcb) and not cn herschel and tc p",
    }

    for title, query in annual_queries.items():
        result = get_annual_counts(query)
        print(f"## {title}\n\n{result}\n\n")

    for title, query in citation_queries.items():
        total, average = get_citations(query)
        print(f"## {title}\n\nTotal: {total}\nAverage: {average}\n\n")


if __name__ == "__main__":
    print_results()
