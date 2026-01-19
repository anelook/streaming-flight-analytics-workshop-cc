# Streaming Flights data. Hands-on with Confluent Cloud: Apache Kafka®, Apache Flink®, and Tableflow
A hands-on workshop using streamed flight data to learn Kafka, Flink, Iceberg, Trino, and Superset. Uses Confluent Cloud.

## 📍Step 1. Set up playground

### 1.1 Open the repository in GitHub Codespace

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](
https://github.com/codespaces/new/anelook/streaming-flight-analytics-workshop-cc)

Alternatively, select the `<> Code` button, go to `Codespaces` and click to `Create codespace on main`.
This will open a codespace project with template files we'll use for the workshop.

We'll be using NodeJS to send data into Apache kafka cluster
In the project, go and rename `.env-example` to `.env`. This is the place where we'll store Kafka connection information.


### 1.2 Get free trial for Confluent Cloud
Register for Confluent Cloud and get free credits by going to [cnfl.io/workshop-cloud](cnfl.io/workshop-cloud).
Once registered, go to Billing and Payment and set the code ``CONFLUENTDEV1``.
todo -add screenshot/gif

## 📍Step 2. Bring the data in!

### 2.1 Create Apache Kafka cluster
On the *Home* page select to **Add Cluster**, use the default environment and choose Basic cluster. This is enough for this workshop.
⚠️ Note: pay attention to provider and region. It is important that we select same provider and region for both Apache Kafka cluster and later for Apache Flink computation pool.

Click "Launch cluster". You'll land on the page that contains basic information about the created cluster. 
Copy the data from the field *Bootstrap server* from *Overview* tab and paste it into .env file into "KAFKA_BROKERS".
Move to *API keys* tab and click **Create key**. Select **My account** and you'll get a pair of a key and a secret. Copy the key into **KAFKA_USERNAME** and the secret into **KAFKA_PASSWORD**.

### 2.2 Create Kafka topic
Move to topics and click to **Create topic**. Set *flight-replay* as the name. Click to *Create with defaults*. You'll get to a page that suggests adding a data contract. Click **Add dat contract**.
Use schema from [avro-schema.json](https://github.com/anelook/streaming-flight-analytics-workshop-cc/blob/main/avro-schema.json). 

### 2.3 Set up schema registry

The schema will be stored in Confluent Cloud and used when storing and reading the records. To access the schema applications need API keys.
To get those go to **Schema Registry**. 
First, get the endpoint from **Endpoint** tab and use it as *SCHEMA_REGISTRY_URL* in .env
In the overview page click on **API keys**, then **+Add API key**, select **My account**, **Schema registry**, default environment and finally **Create API key**.

The key and the secret that you received use for **SCHEMA_REGISTRY_USERNAME** and **SCHEMA_REGISTRY_PASSWORD** correspondingly in `.env` file.

### 2.4 Stream flight data 
Download the **operations.csv.gz** with flight data and unzip it by running this command:
```curl -L https://samples.adsbexchange.com/operations-ax-v2/2025/11/01/operations.csv.gz | gunzip > operations.csv```

Install dependencies:
```npm install```

Run the process to read the data from *operations.csv* for current time and send it to the topic:
```node load-data.js```

Do not stop the process, it imitates streaming real-time data (we just travel in time to October 2025!). Instead, open a new tab to run other commands that we'll need.

You should be able to see data flowing and you can check the records in the interface.
todo -image

## 📍Step 3. Process streaming data with Apache Flink

Let's turn the original Kafka topic into a proper event-time table that all window queries can use.

Must be in the same region/cloud as the cluster!

### Create a new table WITH a watermark

We create a new table that:

- Reads from Kafka
- Uses the same schema
- Adds a watermark on time

```sql
CREATE TABLE flight_replay_rt (
  `time` TIMESTAMP_LTZ(3),
  icao STRING,
  operation STRING,
  airport STRING,
  registration STRING,
  flight STRING,
  ac_type STRING,
  runway STRING,
  flight_link STRING,
  squawk STRING,
  signal_type STRING,
  category STRING,
  `year` INT,
  manufacturer STRING,
  `model` STRING,
  ownop STRING,
  faa_pia BOOLEAN,
  faa_ladd BOOLEAN,
  short_type STRING,
  mil BOOLEAN,
  apt_type STRING,
  `name` STRING,
  continent STRING,
  iso_country STRING,
  iso_region STRING,
  municipality STRING,
  scheduled_service BOOLEAN,
  iata_code STRING,
  elev INT,
  time_original STRING,
  WATERMARK FOR `time` AS `time` - INTERVAL '5' SECOND
)
```

Next insert the data into it:
```
INSERT INTO flight_replay_rt (
  `time`,
  icao,
  operation,
  airport,
  registration,
  flight,
  ac_type,
  runway,
  flight_link,
  squawk,
  signal_type,
  category,
  `year`,
  manufacturer,
  `model`,
  ownop,
  faa_pia,
  faa_ladd,
  short_type,
  mil,
  apt_type,
  `name`,
  continent,
  iso_country,
  iso_region,
  municipality,
  scheduled_service,
  iata_code,
  elev
)
SELECT
  `time`,
  icao,
  operation,
  airport,
  registration,
  flight,
  ac_type,
  runway,
  flight_link,
  squawk,
  signal_type,
  category,
  `year`,
  manufacturer,
  `model`,
  ownop,
  faa_pia,
  faa_ladd,
  short_type,
  mil,
  apt_type,
  `name`,
  continent,
  iso_country,
  iso_region,
  municipality,
  scheduled_service,
  iata_code,
  elev
FROM `flight-replay`;
```

## 📍Step 4. Store in data lake with Apache Iceberg & Tableflow



## 📍Step 5. Query and visualise with Trino and Superset


```docker compose up -d```


If any errors in the *tableflow.properties* file - update the config and restart Trino

```docker compose restart trino```



```docker exec -it trino trino --execute "SHOW SCHEMAS FROM tableflow"```


```docker exec -it trino trino --execute 'SELECT count(*) AS n FROM tableflow."<CHANGE_TO_YOUR_SCHEMA>".flight_ops_rt'```



```docker exec -it trino trino --execute 'SELECT count(*) AS n FROM tableflow."<CHANGE_TO_YOUR_SCHEMA>".flight_ops_rt'```


```
docker exec -it superset bash -lc "pip install sqlalchemy-trino trino"
```

```
docker restart superset
```




## 📍Step 6. Cleanup

