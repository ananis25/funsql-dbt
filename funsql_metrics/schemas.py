from dataclasses import dataclass
from typing import Literal, Optional


DimensionType = Literal["number", "string", "bool", "timestamp"]
MetricType = Literal["avg", "count", "count_distinct", "max", "min", "sum", "number"]
JoinType = Literal["1:1", "1:N", "N:1"]


@dataclass
class Dimension:
    dimension: str
    type: DimensionType
    sql: str
    primary_key: bool = False


@dataclass
class Join:
    join: str
    type: JoinType
    sql: str


@dataclass
class Metric:
    metric: str
    type: MetricType
    sql: str
    filter_: Optional[str]
    label: str
    description: Optional[str]


@dataclass
class Grain:
    grain: str
    table: Optional[str]
    derived_table: Optional[str]
    dimensions: list[Dimension]
    metrics: list[Metric]
    joins: list[Join]

    def validate(self):
        # only one of table or derived table should be declared
        table = self.table
        derived_table = self.derived_table
        if table is not None and derived_table is not None:
            raise ValueError("only one of table or derived_table can be specified")
        elif table is None and derived_table is None:
            raise ValueError("either table or derived_table must be specified")

        # no duplicate dimensions are present
        set_dimensions = set()
        for dim in self.dimensions:
            if dim.dimension in set_dimensions:
                raise ValueError("duplicate dimension: " + dim.dimension)
            set_dimensions.add(dim.dimension)

        # no duplicate metrics are present
        set_metrics = set()
        for metric in self.metrics:
            if metric.metric in set_metrics:
                raise ValueError("duplicate metric: " + metric.metric)
            set_metrics.add(metric.metric)

        # no duplicate joins are present
        set_joins = set()
        for join in self.joins:
            if join.join in set_joins:
                raise ValueError("duplicate join: " + join.join)
            set_joins.add(join.join)

        # a single primary key is declared
        num_pkeys = sum(1 for d in self.dimensions if d.primary_key)
        if num_pkeys > 1:
            raise ValueError("a model must have exactly one primary key dimension")
