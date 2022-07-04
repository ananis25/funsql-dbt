import os
import sqlite3
from functools import wraps

from funsql import *
from funsql.tools import dialect_sqlite


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
DB_FILE = "/tmp/funsql_function_models.db"

# -----------------------------------------------------------
# Prefect utilities
# -----------------------------------------------------------


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


def materialize(table_name: str):
    """Decorator for prefect task"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # create prefect task to create a sqlite table for the query
            # make a query to fetch the schema of the table
            # create an SQLTable object from the output and return

            query: SQLNode = func(*args, **kwargs)
            query_rendered: SQLString = render(query, catalog=DB_CATALOG)  # type: ignore
            col_names = db_create_table(table_name, query_rendered.query)
            return From(
                SQLTable(S(table_name), [S(col_name) for col_name in col_names])
            )  # wrapped in a From node, since the children query uses it directly

        return wrapper

    return decorator


# -----------------------------------------------------------
# Define the views/tables that need to be materialized in the
# database
# -----------------------------------------------------------


def get_stg_orders():
    return From(S.raw_orders) >> Select(
        aka(Get.id, "order_id"),
        aka(Get.user_id, "customer_id"),
        Get.order_date,
        Get.status,
    )


def get_stg_customers():
    return From(S.raw_customers) >> Select(
        aka(Get.id, "customer_id"),
        Get.first_name,
        Get.last_name,
    )


def get_stg_payments():
    return (
        From(S.raw_payments)
        >> Select(
            aka(Get.id, "payment_id"), Get.order_id, Get.payment_method, Get.amount
        )
        >> Define(Fun("/", Get.amount, 100) >> As(S.amount))
    )


def get_customer_orders():
    orders = get_stg_orders()
    return (
        orders
        >> Group(Get.customer_id)
        >> Select(
            Get.customer_id,
            aka(Agg.min(Get.order_date), "first_order"),
            aka(Agg.max(Get.order_date), "most_recent_order"),
            aka(Agg.count(Get.order_id), "number_of_orders"),
        )
    )


@materialize("customer_payments")
def get_customer_payments():
    orders = get_stg_orders()
    payments = get_stg_payments() >> As(S.payments)
    return (
        orders
        >> Join(payments, on=Fun("=", Get.order_id, Get.payments.order_id))
        >> Group(Get.customer_id)
        >> Select(
            Get.customer_id,
            aka(Agg.sum(Get.payments.amount), "total_amount"),
        )
    )


def _join_on_key(q1, q2, q2_alias, key: str):
    """utility routine to show how to construct concise abstractions for queries"""
    return q1 >> Join(
        q2 >> As(q2_alias), on=Fun("=", Get(key), Get(q2_alias) >> Get(key)), left=True
    )  # giving q2 an alias is necessary to disambiguate columns, if q1 and q2 share any


@materialize("customer_final")
def get_customer_final():
    customers = get_stg_customers()
    customer_orders = get_customer_orders()
    customer_payments = get_customer_payments()

    full_table = _join_on_key(
        _join_on_key(customers, customer_orders, "orders", "customer_id"),
        customer_payments,
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


@materialize("order_payments")
def get_order_payments():
    payments = get_stg_payments()

    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]
    pay_cases = [
        aka(
            Agg.sum(Fun.case(Fun("=", Get.payment_method, method), Get.amount, 0)),
            f"{method}_amount",
        )
        for method in payment_methods
    ]

    return (
        payments
        >> Group(Get.order_id)
        >> Select(Get.order_id, *pay_cases, aka(Agg.sum(Get.amount), "total_amount"))
    )


@materialize("orders_final")
def get_orders_final():
    orders = get_stg_orders()
    payments = get_order_payments()

    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]
    return _join_on_key(orders, payments, "payments", "order_id") >> Select(
        Get.order_id,
        Get.customer_id,
        Get.order_date,
        Get.status,
        *[Get.payments >> Get(f"{method}_amount") for method in payment_methods],
        aka(Get.payments.total_amount, "amount"),
    )


def run_final_models():
    get_customer_final()
    get_orders_final()


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

    # run the prefect flow, which should recursively materialize the marked table deps
    run_final_models()
