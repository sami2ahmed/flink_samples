# [Supplement to Confluent Flink Webinar](https://www.confluent.io/resources/online-talk/leveraging-flink-for-data-streaming-in-financial-services/)

## Prerequisities: 
1. [Provision basic confluent cloud cluster](https://docs.confluent.io/cloud/current/clusters/cluster-types.html#basic-cluster)
2. [Provision “stocks” datagen connector](https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html#quick-start)
3. Note this step will take a moment. [Provision Flink cluster](https://docs.confluent.io/cloud/current/flink/get-started/quick-start-cloud-console.html)
4. Select the environment that holds the topics from the stocks datagen 
5. OPTIONALLY: create additional [datagen connector “users”](https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html#datagen-source-connector-for-ccloud) to run the JOIN example above
6. Run the flink SQL code examples below in the Flink workspace 


## Flink SQL code
1. Below is SUM example in Flink SQL using sample market data from the stocks datagen connector: 

Assume we have a Kafka topic titled `sample_data` with the following schema: 

```sql

### Schema:
- `side`: STRING
- `quantity`: INT
- `symbol`: STRING
- `price`: INT
- `account`: STRING
- `userid`: STRING

```

This Flink SQL query sums up the quantity for each symbol:
```sql
SELECT symbol, SUM(quantity) AS sum_by_symbol
FROM sample_data 
GROUP BY symbol;
```

2. Below is an AGGREGATION Flink SQL example: 

This query finds the maximum, minimum, and average price for each symbol.

```sql
SELECT 
    symbol, 
    MAX(price) AS max_price, 
    MIN(price) AS min_price, 
    AVG(price) AS avg_price
FROM sample_data 
GROUP BY symbol;
```

3. Example of how you might bucket market data by price of each symbol: 

This query categorizes price into bins based on symbol. Assume we have bins for prices: low (0-300), medium (300-500), high (500-700).
```sql 
SELECT symbol, 
CASE 
    WHEN price BETWEEN 0 AND 300 THEN 'low'
    WHEN price BETWEEN 300 AND 500 THEN 'medium'
    WHEN price BETWEEN 500 AND 700 THEN 'high'
    ELSE 'very high'
END AS price_category
FROM sample_data;
```
4. JOIN: Assume we have another Kafka topic say “sample_data_1” with the schema: 

### Schema:
   - userid STRING,
   - registertime INTEGER,
   - regionid STRING,
   - gender STRING,

We can do a JOIN on userid with the “sample_data” topic from before. `a` and `b` are aliases for `sample_data` and `sample_data_1`, respectively. The result set of this query will have columns for symbol, price, gender, and regionid. It will contain data for each userid that exists in both `sample_data` and `sample_data_1` topics:

```sql
SELECT a.symbol, a.price, b.gender, b.regionid
FROM sample_data AS a 
JOIN sample_data_1 AS b 
ON a.userid = b.userid;
```