from typing import Any
import click
from pathlib import Path
from ralsei import (
    AddColumnsSql,
    Column,
    CreateTableSql,
    MapToNewTable,
    Ralsei,
    Pipeline,
    Table,
    ValueColumn,
    add_to_input,
    compose,
    compose_one,
    folder,
    pop_id_fields,
)
from csv import DictReader

from ralsei.task import MapToNewColumns
from requests.sessions import Session, HTTPAdapter
from urllib3.util import Retry

from func.padded_csv import HeaderLength, header_length_arg, padded_csv_lines
from func.field_map import authors_fields, publication_fields
from func.split_authors import split_authors
from func.openalex_download import openalex_download, openalex_split


def check_null(value: Any):
    return None if value in {"", "-"} else value


def upload_csv(loc: tuple[Path, HeaderLength], keymap: dict[str, str]):
    path, header_length = loc
    with path.open() as file:
        for row in DictReader(padded_csv_lines(file, header_length)):
            yield {
                keymap[key]: check_null(value)
                for key, value in row.items()
                if key in keymap
            }


OPENALEX_PER_PAGE = 50


@click.option("--authors", "-a", type=(Path, header_length_arg), required=True)
@click.option("--publications", "-p", type=(Path, header_length_arg), required=True)
class AuthorsPipeline(Pipeline):
    def __init__(
        self,
        authors: tuple[Path, HeaderLength],
        publications: tuple[Path, HeaderLength],
    ) -> None:
        self.authors = authors
        self.publications = publications

    def create_tasks(self):
        session = Session()
        adapter = HTTPAdapter(max_retries=Retry(total=5))
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return {
            "authors_raw": MapToNewTable(
                table=Table("work_authors_raw"),
                columns=[*authors_fields.values()],
                fn=compose(
                    upload_csv,
                    add_to_input(
                        loc=self.authors,
                        keymap={key: col.name for key, col in authors_fields.items()},
                    ),
                ),
            ),
            "publications": MapToNewTable(
                table=Table("work_publications"),
                columns=[
                    "publication_id INTEGER PRIMARY KEY",
                    *publication_fields.values(),
                ],
                fn=compose(
                    upload_csv,
                    add_to_input(
                        loc=self.publications,
                        keymap={
                            key: col.name for key, col in publication_fields.items()
                        },
                    ),
                ),
            ),
            "authors": CreateTableSql(
                table=Table("work_authors"),
                sql=folder().joinpath("./func/authors.sql").read_text(),
                params={"raw": self.outputof("authors_raw")},
            ),
            "publication_authors": {
                "split": MapToNewTable(
                    source_table=self.outputof("publications"),
                    select="SELECT publication_id, authors, scopus_author_ids FROM {{source}} WHERE authors IS NOT NULL",
                    table=Table("work_publication_authors"),
                    columns=[
                        ValueColumn("publication_id", "INT REFERENCES {{source}}"),
                        ValueColumn("author_name", "TEXT"),
                        ValueColumn("scopus_author_id", "INT"),
                    ],
                    fn=compose(split_authors, pop_id_fields("publication_id")),
                ),
                "connect": AddColumnsSql(
                    table=self.outputof("publication_authors.split"),
                    sql=folder()
                    .joinpath("./func/publication_authors_connect.sql")
                    .read_text(),
                    params={"authors": self.outputof("authors")},
                ),
                "mark_with_external": AddColumnsSql(
                    table=self.outputof("publications"),
                    sql=folder()
                    .joinpath("./func/mark_with_external_authors.sql")
                    .read_text(),
                    params={"pairs": self.outputof("publication_authors.connect")},
                ),
            },
            "external_authors": {
                "create": CreateTableSql(
                    table=Table("work_external_authors"),
                    sql=folder().joinpath("./func/external_authors.sql").read_text(),
                    params={"pairs": self.outputof("publication_authors.connect")},
                ),
                "connect": AddColumnsSql(
                    table=self.outputof("publication_authors.split"),
                    sql=folder()
                    .joinpath("./func/publication_external_authors_connect.sql")
                    .read_text(),
                    params={"ext_authors": self.outputof("external_authors.create")},
                ),
            },
            "author_count": AddColumnsSql(
                table=self.outputof("external_authors.connect"),
                sql=folder().joinpath("./func/ext_author_count.sql").read_text(),
            ),
            "defined_author_count": AddColumnsSql(
                table=self.outputof("author_count"),
                sql=folder().joinpath("./func/defined_author_count.sql").read_text(),
            ),
            "rank_authors": AddColumnsSql(
                table=self.outputof("defined_author_count"),
                sql=folder().joinpath("./func/rank_pairs.sql").read_text(),
            ),
            "rank_sum": AddColumnsSql(
                table=self.outputof("publications"),
                sql=folder().joinpath("./func/rank_sum.sql").read_text(),
                params={"pairs": self.outputof("rank_authors")},
            ),
            "openalex_work_json": {
                "group": CreateTableSql(
                    sql=folder().joinpath("./func/group_dois.sql").read_text(),
                    table=Table("openalex_queries"),
                    params={
                        "publications": self.outputof(
                            "publication_authors.mark_with_external"
                        ),
                        "per_page": OPENALEX_PER_PAGE,
                    },
                ),
                "download": MapToNewColumns(
                    table=self.outputof("openalex_work_json.group"),
                    select="SELECT query_id, dois FROM {{table}} WHERE NOT {{is_done}}",
                    columns=[ValueColumn("json", "TEXT")],
                    is_done_column="_downloaded",
                    fn=compose_one(
                        openalex_download,
                        pop_id_fields("query_id"),
                        add_to_input(per_page=OPENALEX_PER_PAGE),
                        context={"sess": session},
                    ),
                ),
                "split": MapToNewTable(
                    source_table=self.outputof("openalex_work_json.download"),
                    select="SELECT json AS json_str FROM {{source}}",
                    table=Table("openalex_jsons"),
                    columns=[
                        ValueColumn("doi", "TEXT"),
                        ValueColumn("json", "TEXT"),
                    ],
                    fn=openalex_split,
                ),
                "connect": AddColumnsSql(
                    table=self.outputof("openalex_work_json.split"),
                    columns=[
                        Column("publication_id", "INT REFERENCES {{publications}}")
                    ],
                    sql="""\
                    UPDATE {{table}} AS t
                    SET publication_id = p.publication_id
                    FROM {{publications}} AS p
                    WHERE t.doi = p.doi
                    """,
                    params={"publications": self.outputof("publications")},
                ),
            },
        }


app = Ralsei(AuthorsPipeline)
if __name__ == "__main__":
    app()
