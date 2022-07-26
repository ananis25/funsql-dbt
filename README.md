# funsql-examples

Example notebooks/scripts to demonstrate working with the [FunSQL library](https://github.com/ananis25/funsql-python). 

* `against-sql`: Jamie Brandon has a super thoughtful [writeup](https://www.scattered-thoughts.net/writing/against-sql/) titled `Against SQL` about how working with SQL is difficult. We show that some of the syntactic limitations can be resolved by using a python DSL like FunSQL. 

* `funsql-metrics`: A LookML like application that lets you specify dimensions/metrics against a database schema, and then make queries against it. FunSQL can be used as a query generator. [INCOMPLETE]

* `funsql-dbt`: DBT like script to go over a dependency tree of query views/tables, and materialize them to a data warehouse. Using FunSQL lets us compose query fragments without a templating language like Jinja. 