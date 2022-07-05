## Metrics layer

Multiple projects provide a `metrics layer` against the database, for ex - [dbt metrics](https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/cloud-metrics-layer), [Transform](https://transform.co/), [Supergrain](https://www.supergrain.com/how-it-works).  Thoughts from what I understand of it below.  


### Notes

<details>

<summary>Click to expand</summary>

The product webpages use the term `Headless BI` or `metrics layer`, both not super informative.  I'd expect the pitch roughly goes like this. 

**The typical analytics pipeline looks like this.**

* Maintain a data warehouse (a relational DB) and push all the transactional data to it, as `source tables`. 

* Data engineers create more tables/views against the source tables, and materialize the commonly used ones.

* Data analysts write SQL queries against these views. 

**Where it gets difficult.** 

* SQL rules but is verbose and doesn't compose very well. So, you have repeated logic across similar business queries, making it hard to maintain. 

* The engineers couldn't have forethought all the possible analytic questions. So, the warehouse tables/views evolve over time. Without a catalog, gets harder for the analysts to consume. 

* SQL rules but the business questions don't directly translate to queries. The analysts still need to be familiar with the _physical layout_ of the data warehouse. 

**What a metrics layer does.** 

* An intermediate metrics layer serves as a catalog of all the data attributes available for analytics. You write `model` definitions that roughly correspond to the tables/views in the general workflow. 

* A model definition includes
  * dimensions - just like the schema for a table/view. Could also extend it with sql expressions defined over the physical columns. 
  * metrics - common aggregations defined over the attributes in that model. Regular sql expression. 
  * joins - lists all the allowed attributes on which this model can be joined with another. 

* Data analytics tools write queries in a concise language, with limited set of verbs. Most of the time you specify the metrics you want to see, and which attributes to aggregate them over.  The queries compile to SQL and get executed againt the data warehouse. 

**How it helps**

* You have a catalog of all the data available for analytics. So, you obviously get lineage, versioning, auditing, that kind of stuff. What I like is that all aggregations and relationships between tables are now defined by the engineers so consumers don't need to dive into the physical layout of the database. 

* The query language for analytics is syntactic sugar for a restricted set of SQL, importantly with no explicit JOINs (the query compiler can figure those out from the model definitions). So, you get rid of concerns like if you were joining with the right table. Business questions also map better to queries, which are now smaller. 

**Could it be done with regular SQL?**

Not sure, and that has to do with SQL not being very compositional. An analytics query might involve an aggregation over a column value when the data is grouped by a related column in another table. For example, 

```sql
SELECT AVG(price) 
FROM orders
JOIN customers
    ON orders.customer_id = customers.id
GROUP BY 
    customers.country
```

The `metrics layer` approach is neat in that you kind of split the SQL definition between the model file and the runtime query. 

```txt
// model definition
- relation: orders JOIN customers ON orders.customer_id = customers.id
- metric: average_order_size = AVG(price)

// runtime query looks like
GET average_order_size BY country
```

The query now look more comprehensible and more approachable to a business user. There is a clear delineation in the scope of what the engineers should deal with vs the analysts, even taking some autonomy away from the latter. Though I can definitely see the benefit from it.  It is like SQL query builders provided by ORMs; execution happens only after the full query definition. 

**Conclusion**

So, the `metrics layer` concept is clearly more work and YMMV in the utility you get from it. It does introduce discipline to the analytics workflow and better... umm, separation of concerns between engineering and analysts.  Anyway, the concept make for a good primitive when setting up an analytics workflow from scratch. 

</details>

<br>

### Implementation

The core idea is interesting so we try implement their workflow; no warehouses, just with some data in a sqlite file. 