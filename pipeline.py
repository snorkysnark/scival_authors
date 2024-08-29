from typing import Any
import click
from pathlib import Path
from ralsei import (
    AddColumnsSql,
    CreateTableSql,
    MapToNewTable,
    Ralsei,
    Pipeline,
    Table,
    ValueColumn,
    add_to_input,
    compose,
    folder,
    pop_id_fields,
)
from csv import DictReader

from func.padded_csv import HeaderLength, header_length_arg, padded_csv_lines
from func.field_map import authors_fields, publication_fields
from func.split_authors import split_authors


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
        }


app = Ralsei(AuthorsPipeline)
if __name__ == "__main__":
    app()
