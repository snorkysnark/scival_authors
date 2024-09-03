from ralsei import CreateTableSql, OutputOf, Pipeline, Table, folder


class CsmlPipeline(Pipeline):
    def __init__(
        self,
        publications: OutputOf,
        openalex_publications: OutputOf,
        openalex_jsons: OutputOf,
        authors: OutputOf,
        external_authors: OutputOf,
        publication_authors: OutputOf,
        openalex_authors: OutputOf,
        openalex_authors_raw: OutputOf,
        openalex_affiliations: OutputOf,
    ) -> None:
        self.publications = publications
        self.openalex_publications = openalex_publications
        self.openalex_jsons = openalex_jsons
        self.authors = authors
        self.publication_authors = publication_authors
        self.external_authors = external_authors
        self.openalex_authors = openalex_authors
        self.openalex_authors_raw = openalex_authors_raw
        self.openalex_affiliations = openalex_affiliations

    def create_tasks(self):
        return {
            "source": CreateTableSql(
                sql=folder().joinpath("./csml_source.sql").read_text(),
                table=Table("csml_source"),
            ),
            "type_database_record": CreateTableSql(
                sql=folder().joinpath("./csml_type_database_record.sql").read_text(),
                table=Table("csml_type_database_record"),
            ),
            "type_state_load": CreateTableSql(
                sql=folder().joinpath("./csml_type_state_load.sql").read_text(),
                table=Table("csml_type_state_load"),
            ),
            "record": CreateTableSql(
                sql=folder().joinpath("./csml_record.sql").read_text(),
                table=Table("csml_record"),
                params={
                    "type_database_record": self.outputof("type_database_record"),
                    "source": self.outputof("source"),
                    "type_state_load": self.outputof("type_state_load"),
                    "publications": self.publications,
                    "openalex": self.openalex_publications,
                },
            ),
            "record_relations": CreateTableSql(
                sql=folder().joinpath("./csml_record_relation.sql").read_text(),
                table=Table("csml_record_relation"),
                params={
                    "records": self.outputof("record"),
                    "pairs": self.openalex_jsons,
                },
            ),
            "author": CreateTableSql(
                sql=folder().joinpath("./csml_record_author.sql").read_text(),
                table=Table("csml_record_author"),
                params={
                    "record": self.outputof("record"),
                    "authors": self.authors,
                    "external_authors": self.external_authors,
                    "openalex_authors": self.openalex_authors,
                    "pairs": self.publication_authors,
                    "openalex_authors_raw": self.openalex_authors_raw,
                },
            ),
            "affiliation": CreateTableSql(
                table=Table("csml_record_affiliation"),
                sql=folder().joinpath("./csml_record_affiliation.sql").read_text(),
                params={
                    "affiliations": self.openalex_affiliations,
                    "csml_author": self.outputof("author"),
                    "record": self.outputof("record"),
                },
            ),
            "author_affiliation": CreateTableSql(
                table=Table("csml_record_author_rel_affiliation"),
                sql=folder()
                .joinpath("./csml_record_author_rel_affiliation.sql")
                .read_text(),
                params={
                    "openalex": self.openalex_affiliations,
                    "csml_author": self.outputof("author"),
                    "csml_record_affiliation": self.outputof("affiliation"),
                },
            ),
        }
