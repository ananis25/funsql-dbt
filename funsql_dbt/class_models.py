import os
import sqlite3
from typing import Any

from funsql import *
from funsql.tools import dialect_sqlite

from lib.with_classes import DataModel, Context, populate_tables


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


if __name__ == "__main__":
    # set up the sqlite database if it didn't exist
    DB_FILE = "/tmp/funsql_class_models.db"
    db_exists = os.path.exists(DB_FILE)
    if not db_exists:
        import pandas as pd

        # create tables for csv files
        conn = sqlite3.connect(DB_FILE)
        pd.read_csv("raw_customers.csv").to_sql("raw_customers", conn)
        pd.read_csv("raw_orders.csv").to_sql("raw_orders", conn)
        pd.read_csv("raw_payments.csv").to_sql("raw_payments", conn)

    populate_tables(
        [orders_final, customer_final],
        {
            "db_path": DB_FILE,
            "catalog": DB_CATALOG,
            "payment_methods": ["credit_card", "coupon", "bank_transfer", "gift_card"],
        },
    )
