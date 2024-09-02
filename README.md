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
5. Temporal JOIN example. The below will perform a JOIN between two topics based on eventtime (time field nested in the message body) under the condition that one of the topics will occasionally have a message with an eventtime ahead of the other topic, but we only want the JOIN to execute when the eventtime for messages in both topics match:

Example, two topics called `orders` and `market` respectively:
- `orders` m1 == 9:00, m2 == 9:01, m3 == 9:02
- `market` m1 == 9:00, m2 == 9:02, m3 == 9:04

In the case above, we want the JOIN to execute between `market` and `orders` topics only once the message with a matching eventtime lands in the `orders` topic (which in this case is m3). We do not wish to declare a really long watermark grace period because then our latency would consequently go up, not ideal for trading indicators use case such as this. 

To solve for this, the below is a Flink SQL and a temporal join to market data with orders data. When creating the market data table, I made sure there is a key and a watermark defined. When creating the orders table, I simply made sure no key is defined. The result was whenever an order came in, it was joined with the most recent market data point. If you are new to temporal JOINs, take a look at this before diving into the code below: [Temporal Joins in Flink](https://www.youtube.com/watch?v=ChiAXgTuzaA)

```sql
CCREATE TABLE marketdata(
   `symbol` STRING,
   `latestPrice` DOUBLE,
   `openprice` DOUBLE, 
   `closeprice` DOUBLE,
   `high` DOUBLE,
   `low` DOUBLE,
   `volume` DOUBLE,
   `lastTradeTime` TIMESTAMP_LTZ(3),
   PRIMARY KEY(`symbol`) NOT ENFORCED,
   watermark for `lastTradeTime` as lastTradeTime - INTERVAL '10' SECONDS
)WITH (
    'kafka.partitions' = '1'
);

INSERT INTO marketdata
    SELECT
   JSON_VALUE(data, '$[0].symbol') as SYMBOL, 
   CAST(JSON_VALUE(data, '$[0].latestPrice') as DOUBLE) as LATESTPRICE, 
   CAST(JSON_VALUE(data, '$[0].open') as DOUBLE) as OPENPRICE, 
   CAST(JSON_VALUE(data, '$[0].close') as DOUBLE) as CLOSEPRICE, 
   CAST(JSON_VALUE(data, '$[0].high') as DOUBLE) as HIGH, 
   CAST(JSON_VALUE(data, '$[0].low') as DOUBLE) as LOW, 
   CAST(JSON_VALUE(data, '$[0].volume') as DOUBLE) as VOLUME, 
   TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(data, '$[0].lastTradeTime') as BIGINT), 3) as LASTTRADETIME
FROM `marketdata-raw` where JSON_VALUE(data, '$[0].symbol') IS NOT NULL;

CREATE TABLE orders_nk(
   `orderId` INT,
   `side` STRING,
   `quantity` DOUBLE,
   `symbol` STRING,
   `account` STRING,
   `userid` STRING
)WITH (
    'kafka.partitions' = '1'
    );


INSERT INTO orders_nk 
    select 
        orderId,
        side,
        quantity,
        symbol,
        account,
        userid
    from `orders-raw`;


CREATE TABLE ordersenriched(
     ORDERID INT,
     QUANTITY DOUBLE,
     SYMBOL STRING,
     USERID STRING,
     MARKETPRICE DOUBLE,
     TOTALCOST DOUBLE
) WITH (
    'kafka.partitions' = '1'
    );

INSERT INTO ordersenriched
SELECT
     o.orderId as ORDERID,
     o.quantity as QUANTITY,
     o.symbol as SYMBOL,
     o.userid as USERID,
     marketdata.latestPrice as MARKETPRICE,
     (marketdata.latestPrice * o.quantity) as TOTALCOST
FROM orders_nk o
LEFT JOIN marketdata FOR SYSTEM_TIME AS OF `o`.$rowtime
ON o.symbol = marketdata.symbol;
```