from dataclasses import dataclass, field
from tempfile import NamedTemporaryFile
from pathlib import Path
from pandas import DataFrame
from neopylib.Entities.GraphEntities import Node

PERIODIC_COMMIT = "USING PERIODIC COMMIT "


@dataclass
class Query:
    neo4j_import_basedir: str
    neo4j_import_subdir: str = ""

    def __post_init__(self):
        self.neo4j_import_basedir = Path(self.neo4j_import_basedir)
        self.neo4j_import_dir = Path.joinpath(
            self.neo4j_import_basedir, self.neo4j_import_subdir
        )

    def node_from_pandas(
        self,
        df: DataFrame,
        properties: field(default_factory=set) = set(),
        index=None,
        labels: field(default_factory=set) = {},
    ) -> None:

        datafile = NamedTemporaryFile(
            "w", dir=self.neo4j_import_dir, encoding="utf-8", delete=False
        )
        df.to_csv(datafile)
        self.node_from_file(
            filename=datafile.name, properties=properties, index=index, labels=labels
        )

    def node_from_file(
        self,
        filename: str,
        properties: field(default_factory=set) = set(),
        index: str = None,
        labels: field(default_factory=set) = {},
    ) -> None:

        LOAD = "LOAD CSV WITH HEADERS FROM {filepath} AS row "
        CREATE = "CREATE {node} "
        INDEX = "" if index is None else " CREATE INDEX ON {node_symb} ;"

        self.query = (
            PERIODIC_COMMIT
            + LOAD.format(
                filepath=f"file:///{Path(filename).relative_to(self.neo4j_import_basedir)}"
            )
            + CREATE.format(
                node=Node(
                    properties={p: f"row.{p}" for p in properties}, labels=labels
                ).parse()
            )
            + "; "
            + INDEX.format(node_symb=f":{','.join(labels)}({index})")
        )

    def link_nodes(self, on_left, on_right):
        pass  # TODO

    def execute(self, neo4j_session):
        try:
            return neo4j_session.write_transaction(lambda tx: tx.run(self.query))
        except:
            print("Query not defined error")
            return None

