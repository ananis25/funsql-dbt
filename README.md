# funsql-dbt

This folder hosts a DBT like script to go over a dependency tree of query views/tables, and materialize them to a data warehouse.  The FunSQL library helps compose SQL queries, we could do away with a templating language like Jinja. 

**It isn't particularly desirable!** DBT fits the interactive workflow - models are SQL queries introduced one at a time, with some Jinja markup/macros.  That is a more declarative approach than combining python functions.  

However, writing `transform` steps using a python DSL affords some flexibility.  

* Construct queries dynamically - you can use regular python control flow, choose to only materialize tables that have say, multiple children, share logic/parameters more easily, etc. 
* Using alternate orchestration engines like Prefect/Dagster - you can still get workflow semantics for the whole process, like error recovery and retries, but get more control over the execution. 

I tried to reproduce the `jaffle shop` [example](https://github.com/dbt-labs/jaffle_shop) from the DBT tutorial.  We use a sqlite database file, and no other dependencies.  Though it should be easy to use a workflow tool like `Prefect` to make table materializations as discrete tasks, and get caching/scheduling and other good stuff. 

## Using functions

Change to the `funsql_dbt` directory and run `function_models.py`. 

Each `data model` is a function, that returns a FunSQL query. We decorate the functions to specify if the output of that query should be materialized, and the table name for it.  The task runner takes as input a list of data models to materialize, then descends down the dependency tree and also executes any intermediate models.  

The resulting code is short enough, but setting up model dependencies is clunky.  We could pass them as arguments to each model function, but wiring models together everytime is tedious.  So, instead we call the parent models directly inside the model code, but now we lose any visibility of the dependency graph. That also means execution can only be sequential. 


## Using classes 

Change to the `funsql_dbt` directory and run `class_models.py`.

Each data model is a class object, with the other data models it depends on as attributes.  Now, we can get the dependency graph by inspecting the code, and optimize how to go about generating the tables/views.  

* The task runner get a list of models to materialize. It descends down the dependency tree and creates a topological order over all the models. 

* Now, it can execute the models starting from the ones without any parent models. Wiring up models is also straightforward since inspecting the class definition tells us the dependencies, and topological sort ensures they have been executed first.  With data warehouses that are happy to run concurrent queries, we can also execute models in parallel that are not blocked on any parent models finishing first. 

* To share parameters across models, we create a single context store for all models, and pass it along for all executions. 