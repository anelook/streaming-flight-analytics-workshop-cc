# Streaming Flights data. Hands-on with Confluent Cloud: Apache Kafka®, Apache Flink®, and Tableflow
A hands-on workshop using streamed flight data to learn Kafka, Flink, Iceberg, Trino, and Superset. Uses Confluent Cloud.

## 📍Step 1. Set up playground

### 1.1 Open the repository in GitHub Codespace

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](
https://github.com/codespaces/new/anelook/streaming-flight-analytics-workshop-cc)

Alternatively, select the `<> Code` button, go to `Codespaces` and click to `Create codespace on main`.

### 1.2 Get free trial for Confluent Cloud
Register for Confluent Cloud and get free credits by going to [cnfl.io/workshop-cloud](cnfl.io/workshop-cloud).
Once registered, go to Billing and Payment and set the code ``CONFLUENTDEV1``.
todo -add screenshot/gif

## 📍Step 2. Bring the data in!


### 2.1 Create Apache Kafka cluster

### 2.2 Create Kafka topic

### 2.3 Stream flight data 
```npm install```
```node load-data.js```

## 📍Step 3. Process streaming data with Apache Flink


## 📍Step 4. Store in data lake with Apache Iceberg & Tableflow

## 📍Step 5. Query and visualise with Trino and Superset


```docker compose up -d```


If any errors in the *tableflow.properties* file - update the config and restart Trino

```docker compose restart trino```



```docker exec -it trino trino --execute "SHOW SCHEMAS FROM tableflow"```


```docker exec -it trino trino --execute 'SELECT count(*) AS n FROM tableflow."<CHANGE_TO_YOUR_SCHEMA>".flight_ops_rt'```



```docker exec -it trino trino --execute 'SELECT count(*) AS n FROM tableflow."<CHANGE_TO_YOUR_SCHEMA>".flight_ops_rt'```




## 📍Step 6. Cleanup

