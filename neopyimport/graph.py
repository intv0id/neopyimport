from dataclasses import dataclass, field
from pathlib import Path
from tempfile import NamedTemporaryFile

from neo4j import Session
from neopylib.Entities.GraphEntities import Node
from pandas import DataFrame

PERIODIC_COMMIT = "USING PERIODIC COMMIT "


@dataclass
class Query:
    """Class generating python queries and setting up the environment for importing a dataframe in the neo4j graph database

        Arguments:
            neo4j_import_basedir {str} -- The neo4j import directory absolute path.
            neo4j_import_subdir {str} -- The neo4j import subdirectory path relative to neo4j_import_basedir (default: {""})
    """

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
        properties: set = set(),
        index: str = None,
        labels: set = set(),
    ) -> None:
        """Generate node creating query from a pandas dataset
        
        Arguments:
            df {DataFrame} -- The input pd.DataFrame
        
        Keyword Arguments:
            properties {set} -- Names of the pandas columns wanted as properties (default: {set()})
            index {str} -- The column name set as index for the resulting nodes (default: {None})
            labels {set} -- The list of labels given to the resulting nodes (default: {set()})
        
        Returns:
            None -- The result query is stored in self.query and the processed csv file used by Neo4j for import is stored in the input path specified in the __init__.
        """

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
        properties: set = set(),
        index: str = None,
        labels: set = set(),
    ) -> None:
        """Generate node creation query from a csv file
        
        Arguments:
            filename {str} -- The input file location
        
        Keyword Arguments:
            properties {set} -- Names of the pandas columns wanted as properties (default: {set()})
            index {str} -- The column name set as index for the resulting nodes (default: {None})
            labels {set} -- The list of labels given to the resulting nodes (default: {set()})
        
        Returns:
            None -- The result query is stored in self.query and the processed csv file used by Neo4j for import is stored in the input path specified in the __init__.
        """

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

    def execute(self, neo4j_session: Session):
        """Execute the generated neo4j query
        
        Arguments:
            neo4j_session {neo4j.Session} -- The neo4j Session object used for querying the database.
        """
        if self.query == "":
            raise ValueError(
                "The query must be generated before this method is called."
            )
        try:
            return neo4j_session.write_transaction(lambda tx: tx.run(self.query))
        except:
            print("Query not defined error")
            return None
