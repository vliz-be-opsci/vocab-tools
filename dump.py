#! /usr/bin/env python
from sema.query import GraphSource, DefaultSparqlBuilder, QueryResult
import pandas as pd
from datetime import date
from pathlib import Path
from typing import Dict
from logging import getLogger


log = getLogger(__name__)
TEMPLATES_FOLDER: Path = Path(__file__).parent / "sparql"
sparqlbuilder: DefaultSparqlBuilder = DefaultSparqlBuilder(TEMPLATES_FOLDER)
query: str = sparqlbuilder.build_syntax("skos-concepts.sparql")
SEP: Dict[str, str] = dict(
    cabt="https://id.cabi.org/PoolParty/sparql/cabt",
    asfa="https://agrovoc.fao.org/sparql",
)


def make_skos_concepts_dump(sparql_endpoint: str, file_name: str):
    log.debug(f"making dump for {sparql_endpoint} to {file_name}")
    try:
        # make GraphSource for endpoint
        source: GraphSource = GraphSource.build(sparql_endpoint)
        # exec to df
        result: QueryResult = source.query(query)
        df: pd.DataFrame = result.to_dataframe()
        # ensure location exists
        Path(file_name).parent.mkdir(parents=True, exist_ok=True)
        # write to csv
        df.to_csv(file_name, index=False, encoding="utf-8")
    except Exception as e:
        log.exception(e)  # log but continue


def main():
    today = date.today().isoformat()
    for name, sparql_endoint in SEP.items():
        file_name = f"datadumps/{today}-{name}.csv"
        make_skos_concepts_dump(sparql_endoint, file_name)


if __name__ == "__main__":
    main()
