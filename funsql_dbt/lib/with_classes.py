import sqlite3
from collections import defaultdict
from typing import Any, Optional, ClassVar, Type

from funsql import *


# -----------------------------------------------------------
# utilities to create a graph of data models
# -----------------------------------------------------------


Context = dict[str, Any]


def get_parent_models(kls) -> list[tuple[str, Type["DataModel"]]]:
    return [
        (field, typ)
        for field, typ in kls.__annotations__.items()
        if isinstance(typ, type) and issubclass(typ, DataModel)
    ]


class DataModel:
    __materialize__: ClassVar[bool] = False
    materialized: Optional[SQLNode]

    def __init__(self, *args) -> None:
        fields = [f for f, _ in get_parent_models(self.__class__)]
        for field, arg in zip(fields, args):
            setattr(self, field, arg)
        self.materialized = None

    @classmethod
    def persist(cls) -> bool:
        return cls.__materialize__

    def __call__(self, ctx: Context) -> SQLNode:
        if self.__materialize__ is False or self.materialized is None:
            return self.query(ctx)
        else:
            return self.materialized

    def query(self, ctx: Context) -> SQLNode:
        raise Exception("Not implemented")


# -----------------------------------------------------------
# Set up a prefect flow to materialize all the data models
# -----------------------------------------------------------


def fill_graph(model: Type[DataModel], node_children: dict, node_parent_count: dict):
    """utility routine to show how to construct a graph of data models"""
    if model not in node_parent_count:
        node_children[model] = []
        node_parent_count[model] = 0

    for _, parent in get_parent_models(model):
        node_children[parent].append(model)
        node_parent_count[model] += 1
        fill_graph(parent, node_children, node_parent_count)


def db_create_table(table_name: str, query_rendered: str, ctx: Context) -> list[str]:
    conn = sqlite3.connect(ctx["db_path"])

    with conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    query_str = f"CREATE TABLE {table_name} AS {query_rendered}"
    try:
        with conn:
            conn.execute(query_str)
    except sqlite3.OperationalError as e:
        print(f"query err-ing:\n{query_str}\n")
        raise e

    curr = conn.cursor()
    curr.execute(f"SELECT * FROM {table_name} LIMIT 0")
    col_names: list[str] = [desc[0] for desc in curr.description]
    conn.close()

    return col_names


def populate_tables(models: list[Type[DataModel]], ctx: Context):
    """Figure out all the dependencies for the set of models provided, do a topological
    sort over the full DAG, and then materialize them in the database.
    """
    node_children: dict[Type[DataModel], list[Type[DataModel]]] = defaultdict(list)
    node_parent_count: dict[Type[DataModel], int] = defaultdict(lambda: 0)

    seen = set()
    buffer = [m for m in models]
    while len(buffer) > 0:
        model = buffer.pop(0)
        seen.add(model)
        if model not in node_parent_count:
            node_parent_count[model] = 0

        for _, parent in get_parent_models(model):
            node_children[parent].append(model)
            node_parent_count[model] += 1
            if parent not in seen:
                buffer.append(parent)

    # iterate over the graph in topological order, materializing each model if specified
    results: dict[Type[DataModel], DataModel] = {}
    queue = [node for node in node_parent_count if node_parent_count[node] == 0]

    while len(queue) > 0:
        node = queue.pop(0)
        parents = [results[p] for _, p in get_parent_models(node)]
        node_instance = node(*parents)

        results[node] = node_instance
        query = node_instance(ctx)
        if node.__materialize__:
            query_rendered: SQLString = render(query, catalog=ctx["catalog"])  # type: ignore
            table_name = node.__name__

            col_names = db_create_table(table_name, query_rendered.query, ctx)
            table = SQLTable(S(table_name), [S(col_name) for col_name in col_names])
            # wrapped in a From node, since the children query uses it directly
            node_instance.materialized = From(table)

        # go over children, and add them to the queue if they have no parents remaining
        for child in node_children[node]:
            node_parent_count[child] -= 1
            if node_parent_count[child] == 0:
                queue.append(child)
        node_children.pop(node)
        node_parent_count.pop(node)

    assert (
        len(node_parent_count) == 0
    ), "data models remaining in the graph that were not visited"
