import os
import sqlite3
from collections import defaultdict
from functools import reduce
from typing import Any, Optional, ClassVar, Type

from funsql import *
from funsql.tools import dialect_sqlite


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
# Data models specifid in the sample `jaffle` project in DBT
# tutorials.
# -----------------------------------------------------------

TABLE_CUSTOMERS = SQLTable(S.raw_customers, [S.id, S.first_name, S.last_name])
TABLE_ORDERS = SQLTable(S.raw_orders, [S.id, S.user_id, S.order_date, S.status])
TABLE_PAYMENTS = SQLTable(
    S.raw_payments, [S.id, S.order_id, S.payment_method, S.amount]
)
DB_CATALOG = SQLCatalog(
    dialect=dialect_sqlite(),
    tables={
        S.raw_customers: TABLE_CUSTOMERS,
        S.raw_orders: TABLE_ORDERS,
        S.raw_payments: TABLE_PAYMENTS,
    },
)
DB_FILE = "/tmp/funsql_class_models.db"


class stg_orders(DataModel):
    def query(self, ctx: Context) -> SQLNode:
        return From(S.raw_orders) >> Select(
            aka(Get.id, "order_id"),
            aka(Get.user_id, "customer_id"),
            Get.order_date,
            Get.status,
        )


class stg_customers(DataModel):
    def query(self, ctx: Context) -> SQLNode:
        return From(S.raw_customers) >> Select(
            aka(Get.id, "customer_id"),
            Get.first_name,
            Get.last_name,
        )


class stg_payments(DataModel):
    def query(self, ctx: Context) -> SQLNode:
        return (
            From(S.raw_payments)
            >> Select(
                aka(Get.id, "payment_id"), Get.order_id, Get.payment_method, Get.amount
            )
            >> Define(Fun("/", Get.amount, 100) >> As(S.amount))
        )


class customer_orders(DataModel):
    orders: stg_orders

    def query(self, ctx: Context) -> SQLNode:
        return (
            self.orders(ctx)
            >> Group(Get.customer_id)
            >> Select(
                Get.customer_id,
                aka(Agg.min(Get.order_date), "first_order"),
                aka(Agg.max(Get.order_date), "most_recent_order"),
                aka(Agg.count(Get.order_id), "number_of_orders"),
            )
        )


class customer_payments(DataModel):
    __materialize__ = True
    orders: stg_orders
    payments: stg_payments

    def query(self, ctx: dict[str, Any]) -> SQLNode:
        return (
            self.orders(ctx)
            >> Join(
                self.payments(ctx) >> As(S.payments),
                on=Fun("=", Get.order_id, Get.payments.order_id),
            )
            >> Group(Get.customer_id)
            >> Select(
                Get.customer_id,
                aka(Agg.sum(Get.payments.amount), "total_amount"),
            )
        )


def _join_on_key(q1: SQLNode, q2: SQLNode, q2_alias: str, key: str):
    """utility routine to show how to construct concise abstractions for queries"""
    return q1 >> Join(
        q2 >> As(q2_alias), on=Fun("=", Get(key), Get(q2_alias) >> Get(key)), left=True
    )  # giving q2 an alias is necessary to disambiguate columns, if q1 and q2 share any


class customer_final(DataModel):
    __materialize__ = True
    customers: stg_customers
    customer_orders: customer_orders
    customer_payments: customer_payments

    def query(self, ctx: Context) -> SQLNode:
        full_table = _join_on_key(
            _join_on_key(
                self.customers(ctx), self.customer_orders(ctx), "orders", "customer_id"
            ),
            self.customer_payments(ctx),
            "payments",
            "customer_id",
        )
        return full_table >> Select(
            Get.customer_id,
            Get.first_name,
            Get.last_name,
            Get.orders.first_order,
            Get.orders.most_recent_order,
            Get.orders.number_of_orders,
            aka(Get.payments.total_amount, "customer_lifetime_value"),
        )


class order_payments(DataModel):
    __materialize__ = True
    payments: stg_payments

    def query(self, ctx: Context) -> SQLNode:
        payment_methods: list[str] = ctx["payment_methods"]
        pay_cases = [
            aka(
                Agg.sum(Fun.case(Fun("=", Get.payment_method, method), Get.amount, 0)),
                f"{method}_amount",
            )
            for method in payment_methods
        ]

        return (
            self.payments(ctx)
            >> Group(Get.order_id)
            >> Select(
                Get.order_id, *pay_cases, aka(Agg.sum(Get.amount), "total_amount")
            )
        )


class orders_final(DataModel):
    __materialize__ = True
    orders: stg_orders
    payments: order_payments

    def query(self, ctx: Context) -> SQLNode:
        payment_methods: list[str] = ctx["payment_methods"]
        return _join_on_key(
            self.orders(ctx), self.payments(ctx), "payments", "order_id"
        ) >> Select(
            Get.order_id,
            Get.customer_id,
            Get.order_date,
            Get.status,
            *[Get.payments >> Get(f"{method}_amount") for method in payment_methods],
            aka(Get.payments.total_amount, "amount"),
        )


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


def db_create_table(table_name: str, query_rendered: str):
    conn = sqlite3.connect(DB_FILE)

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
            query_rendered: SQLString = render(query, catalog=DB_CATALOG)  # type: ignore
            table_name = node.__name__

            col_names = db_create_table(table_name, query_rendered.query)
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


if __name__ == "__main__":
    # set up the sqlite database if it didn't exist
    db_exists = os.path.exists(DB_FILE)
    conn = sqlite3.connect(DB_FILE)

    if not db_exists:
        import pandas as pd

        # create tables for csv files
        pd.read_csv("raw_customers.csv").to_sql("raw_customers", conn)
        pd.read_csv("raw_orders.csv").to_sql("raw_orders", conn)
        pd.read_csv("raw_payments.csv").to_sql("raw_payments", conn)

    populate_tables(
        [orders_final, customer_final],
        {"payment_methods": ["credit_card", "coupon", "bank_transfer", "gift_card"]},
    )
