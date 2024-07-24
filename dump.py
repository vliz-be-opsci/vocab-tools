#! /usr/bin/env python
from sema.query import GraphSource, DefaultSparqlBuilder, QueryResult
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict
from logging import getLogger
import sys


log = getLogger(__name__)
TEMPLATES_FOLDER: Path = Path(__file__).parent / "sparql"
sparqlbuilder: DefaultSparqlBuilder = DefaultSparqlBuilder(TEMPLATES_FOLDER)
SEP: Dict[str, str] = dict(
    asfa="https://agrovoc.fao.org/sparql",
    bodc="https://vocab.nerc.ac.uk/sparql/sparql",
    cabt="https://id.cabi.org/PoolParty/sparql/cabt",
)


def ts() -> datetime:
    return datetime.now(tz=timezone.utc)


def make_skos_concepts_dump(sparql_endpoint: str, file_name: str, N: int = None):
    query: str = sparqlbuilder.build_syntax("skos-concepts.sparql", N=N)
    t0 = ts()  # starting point
    tp = None  # previous point in time
    
    def dt(tr: datetime = None) -> int:
        nonlocal tp
        tr = tr or tp or t0  # ref for this calculation
        tn = ts()  # new point in time
        d = (tn - tr) // timedelta(milliseconds=1)
        tp = tn
        return d
        
    print(f"dump of {N if N else 'unlimited'} skos concepts from {sparql_endpoint} to {file_name}")
    try:
        # make GraphSource for endpoint
        source: GraphSource = GraphSource.build(sparql_endpoint)
        # exec to df
        result: QueryResult = source.query(query)
        print(f"  - [qr@{dt()}]", end="", flush=True)
        df: pd.DataFrame = result.to_dataframe()
        print(f"[df@{dt()}]", end="", flush=True)
        # ensure location exists
        Path(file_name).parent.mkdir(parents=True, exist_ok=True)
        # write to csv
        df.to_csv(file_name, index=False, encoding="utf-8")
        print(f"[csv@{dt()}]", end="", flush=True)
    except Exception as e:
        log.exception(e)  # log but continue
    print(f"\n  - dump took {dt(t0)} millis")


def main(size: int = None, *extra_args):
    today = date.today().isoformat()
    for name, sparql_endoint in SEP.items():
        file_name = f"datadumps/{today}-{name}.csv"
        make_skos_concepts_dump(sparql_endoint, file_name, N=size)


if __name__ == "__main__":
    main(*sys.argv[1:])
