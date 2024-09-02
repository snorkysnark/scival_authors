from ralsei import CreateTableSql, Pipeline, Table, folder


class CsmlPipeline(Pipeline):
    def __init__(self) -> None:
        pass

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
                },
            ),
            "author": CreateTableSql(
                sql=folder().joinpath("./csml_record_author.sql").read_text(),
                table=Table("csml_record_author"),
                params={"record": self.outputof("record")},
            ),
        }
