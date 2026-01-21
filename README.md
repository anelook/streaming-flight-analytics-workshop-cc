# Streaming flight data ✈️. Hands-on with Confluent Cloud: Apache Kafka®, Apache Flink®, and Tableflow
This is a hands-on workshop that uses streamed flight data to explore Apache Kafka®, Apache Flink®, Apache Iceberg, Trino, and Superset.
The workshop is built around Confluent Cloud and focuses on learning by doing.

## 📍Step 1. Set up the playground

### 1.1 Open the repository in GitHub Codespaces

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](
https://github.com/codespaces/new/anelook/streaming-flight-analytics-workshop-cc)

Alternatively, click the `<> Code` button in the repository, go to the **Codespaces** tab, and select **Create codespace on main**.

This will open a ready-to-use Codespaces environment with the template files we’ll use throughout the workshop.

We’ll use **Node.js** to send data into an Apache Kafka cluster.  
In the project root, rename `.env-example` to `.env`. This file is where we’ll store the Kafka connection details.



### 1.2 Get a free trial for Confluent Cloud
Register for Confluent Cloud and get free credits by visiting  [cnfl.io/workshop-cloud](cnfl.io/workshop-cloud).
Once you’ve registered, go to **Billing & Payment** in the Confluent Cloud UI and apply the promo code: ``CONFLUENTDEV1``.
todo -add screenshot/gif

## 📍Step 2. Bring the data in!

### 2.1 Create a new environment and an Apache Kafka cluster

Start by creating a new **Environment**. This makes it easier to clean everything up later.

Go to **Environments**, click **Add cloud environment**, keep the **Essentials** category, and give it a name (for example, *Workshop*).
<img width="742" height="578" alt="Screenshot 2026-01-19 at 13 49 14" src="https://github.com/user-attachments/assets/693aef77-13c2-4e42-9199-302dc7f6ecbc" />

Next, create an Apache Kafka cluster:
- Use the newly created environment
- Choose a **Basic** cluster (this is more than enough for this workshop)

⚠️ **Important:** Pay attention to the selected **cloud provider** and **region**.  
We’ll need to use the same provider and region later when creating the Apache Flink compute pool.

Click **Launch cluster**. You’ll land on its overview page.

From the **Overview** tab:
- Copy the value from **Bootstrap server**
- Paste it into your `.env` file as `KAFKA_BROKERS`

Next, go to the **API keys** tab:
- Click **Create key**
- Select **My account**
- You’ll get an API key and secret

Copy:
- the **key** into `KAFKA_USERNAME`
- the **secret** into `KAFKA_PASSWORD`

### 2.2 Create a Kafka topic
Go to **Topics** and click **Create topic**.  
Set the topic name to `flight-replay` and click **Create with defaults**.

After the topic is created, you’ll see a page suggesting that you add a data contract.  
Click **Add data contract** and use the schema from [avro-schema.json](https://github.com/anelook/streaming-flight-analytics-workshop-cc/blob/main/avro-schema.json). 

### 2.3 Set up Schema Registry

The schema will be stored in Confluent Cloud and used when writing and reading records.  
To access Schema Registry, applications need API keys.

Go to **Schema Registry** in the Confluent Cloud UI.

First:
- Open the **Endpoint** tab
- Copy the endpoint URL
- Set it as `SCHEMA_REGISTRY_URL` in your `.env` file

Next, from the **Overview** page:
- Click **API keys**
- Click **+ Add API key**
- Select **My account**
- Choose **Schema Registry**
- Select the `Workshop` environment
- Click **Create API key**

Use the generated values in your `.env` file:
- API key → `SCHEMA_REGISTRY_USERNAME`
- API secret → `SCHEMA_REGISTRY_PASSWORD`

### 2.4 Stream flight data
Download the flight operations dataset and unzip it by running:
```bash
curl -L https://samples.adsbexchange.com/operations-ax-v2/2025/11/01/operations.csv.gz | gunzip > operations.csv
```

Install dependencies:
```npm install```

Start the process that reads data from *operations.csv*, aligns it with the current time, and sends it to the Kafka topic:
```node load-data.js```

⚠️ Important: Do not stop this process.
It simulates real-time streaming (we’re just traveling back to October 2025 😉).

Instead, open a new terminal tab in your Codespace to run the next commands.

At this point, you should see data flowing into the topic, and you can inspect the records directly in the Confluent Cloud UI.
todo -image

## 📍 Step 3. Process streaming data with Apache Flink

Let’s turn the original Kafka topic into a proper **event-time table** that we can use for windowed queries.

To do this, we’ll create an Apache Flink **compute pool**.

Go to your environment overview, click **Compute pools**, then **Create compute pool**.

⚠️ **Important:** Make sure to select the **same cloud provider and region** as the Apache Kafka cluster.

Once the compute pool is ready, click **Open SQL workspace**.  
Select your Kafka cluster as the database, and you’re ready to run SQL queries on live streaming data.

### 3.1 Create a new table *with a watermark*

We’ll create a new table that:

- Reads from Kafka
- Uses the same schema as the source topic
- Adds a watermark on the event-time column

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

We'll be using now the table `flight_replay_rt` for all our experiments.

### 3.2 Enable Tableflow for `flight_replay_rt`.
To save us a bit time later, let's enable Tableflow for `flight_replay_rt`.
Go to the topic page and click on **Enable Tableflow**.
todo add pic

### 3.3 Query streaming data using SQL

Now you can start using SQL to transform and analyze the streaming data in real time.

The dataset uses **ICAO airport codes**, which are commonly used by aviation professionals.  
Let’s begin by finding the **10 busiest airports** based on the total number of operations seen so far.

```sql
SELECT
  airport,
  total_ops
FROM (
  SELECT
    airport,
    COUNT(*) AS total_ops
  FROM flight_replay_rt
  WHERE airport IS NOT NULL
  GROUP BY airport
)
ORDER BY total_ops DESC
LIMIT 10;
```
You’ll most likely see some of the following airports in the results:

EHAM – Amsterdam Airport Schiphol (Netherlands)
RJTT – Haneda Airport (Tokyo, Japan)
EGLL – Heathrow Airport (London, UK)
KMEM – Memphis International Airport (USA)
EDDF – Frankfurt Airport (Germany)

Next, let’s look at a rolling 10-minute view of operations using hopping windows:
```sql
SELECT
  window_start,
  window_end,
  operation,
  COUNT(*) AS cnt
FROM TABLE(
  HOP(TABLE flight_replay_rt, DESCRIPTOR(`time`), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE)
)
GROUP BY window_start, window_end, operation
ORDER BY window_start DESC, cnt DESC
LIMIT 200;
```

Next, let’s split landings vs. takeoffs per minute using tumbling windows:
```sql
SELECT
  window_start,
  operation,
  COUNT(*) AS cnt
FROM TABLE(
  TUMBLE(TABLE flight_replay_rt, DESCRIPTOR(`time`), INTERVAL '1' MINUTE)
)
GROUP BY window_start, operation
ORDER BY window_start DESC, cnt DESC
LIMIT 200;

```

### 3.4 Next let's look at ML examples (forecasting + anomaly detection).

Forecast total operations per minute for one airport (ML_FORECAST)
This query builds a per-minute time series per airport and then uses `ML_FORECAST` to predict the next few minutes.

```sql
WITH per_minute AS (
    SELECT
        airport,
        window_time AS `time`,
        COUNT(*) AS ops_per_minute
    FROM TABLE(
            TUMBLE(TABLE flight_replay_rt, DESCRIPTOR(`time`), INTERVAL '1' MINUTE)
         )
    GROUP BY airport, window_start, window_end, window_time
)

SELECT
    airport,
    `time`,
    ops_per_minute,
    -- ARIMA-based forecast, Auto-ARIMA with a small minTrainingSize for demo
    ML_FORECAST(
            CAST(ops_per_minute AS DOUBLE),
            `time`,
            JSON_OBJECT(
                    'minTrainingSize' VALUE 30,        -- start forecasting after ~30 points :llmCitationRef[2]
                    'enableStl'      VALUE TRUE,       -- handle daily patterns if any :llmCitationRef[3]
                    'horizon'        VALUE 5           -- predict 5 minutes ahead :llmCitationRef[4]
            )
    ) OVER (
    PARTITION BY airport
    ORDER BY `time`
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS forecast
FROM per_minute
WHERE airport = 'EHAM';
```
We set `horizon` to *5*, so for each historical row, ML_FORECAST produces 5 forecast steps into the future. Each step has fields like:
```bash
forecast timestamp, forecasted value, lower bound, upper bound, RMSE, AIC
2026-01-21 11:23:59.999000,2.0,1.0,3.0,0.6324555320336759,11.607931672675951,
2026-01-21 11:24:59.999000,1.0,0.0,2.0,0.6324555320336759,11.607931672675951,
2026-01-21 11:25:59.999000,2.0,1.0,3.0,0.6324555320336759,11.607931672675951,
2026-01-21 11:26:59.999000,1.0,0.0,2.0,0.6324555320336759,11.607931672675951,
2026-01-21 11:27:59.999000,2.0,1.0,3.0,0.6324555320336759,11.607931672675951
```

Next, let's detect spikes in takeoffs per 5-minute window (ML_DETECT_ANOMALIES)
This query looks at takeoffs from EHAM, aggregated into 5-minute windows, and flags values that fall outside the expected range.

```sql
WITH takeoffs_5min AS (
    SELECT
        airport,
        window_time AS `time`,
        COUNT(*) AS takeoffs_per_5min
    FROM TABLE(
            TUMBLE(TABLE flight_replay_rt, DESCRIPTOR(`time`), INTERVAL '5' MINUTE)
         )
    WHERE operation = 'takeoff'
      AND airport = 'EHAM'
    GROUP BY airport, window_start, window_end, window_time
)

SELECT
    airport,
    `time`,
    takeoffs_per_5min,
    ML_DETECT_ANOMALIES(
            CAST(takeoffs_per_5min AS DOUBLE),
            `time`,
            JSON_OBJECT(
                    'minTrainingSize'      VALUE 10,      -- train after ~30 windows :llmCitationRef[6]
                    'enableStl'           VALUE FALSE,
                    'confidencePercentage' VALUE 95.0     -- 95% band: less strict than 99% :llmCitationRef[7]
            )
    ) OVER (
    ORDER BY `time`
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS anomaly
FROM takeoffs_5min;
```
The returned anomaly row includes fields like `forecast_value`, `lower_bound`, `upper_bound`, `rmse`, `aic`, `timestamp`, and `is_anomaly`.
`is_anomaly` becomes TRUE when the actual count falls outside the confidence band.


## 📍 Step 4. Store the data in the data lake with Apache Iceberg & Tableflow

Tableflow takes care of moving the streamed data into Apache Iceberg tables behind the scenes.  
Once this step is in place, your flight data is safely stored in a data lake and ready to be queried.

## 📍 Step 5. Query and visualize with Trino and Superset

In this section, we’ll set up [Trino](https://trino.io/) to query data stored in Iceberg, and use [Apache Superset™](https://superset.apache.org/) to visualize the results.


### 5.1 Trino configuration

To configure Trino, we need to give it access to Confluent Cloud.

Open the file `trino/catalog/tableflow.properties` in the repository.  
This configuration is used by Docker Compose when starting the Trino container.

You’ll need the following information:
- Tableflow endpoint address
- an API key and secret
- the region of your Apache Kafka cluster

You can find the endpoint address and create API keys in the **Tableflow** page in Confluent Cloud.  
Create the API keys the same way as before, and use them to populate the values in `tableflow.properties`..

```bash
# tableflow.properties
# additional documentation can be found here - https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/query-with-trino.html

connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.oauth2.credential=<YOUR-API-KEY>:<YOUR-API-SECRET>
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.uri=<YOUR-REST-CATALOG-ENDPOINT>
# REST Catalog URI Example: https://tableflow.{CLOUD_REGION}.aws.confluent.cloud/iceberg/catalog/organizations/{ORG_ID}/environments/{ENV_ID}
iceberg.rest-catalog.vended-credentials-enabled=true

fs.native-s3.enabled=true
s3.region=<YOUR-CONFLUENT-CLUSTER-REGION>
# S3 Region Example: us-west-2

iceberg.security=read_only
```

### 5.2 Start Trino and Superset with Docker Compose

The Docker configuration is defined in `docker-compose.yml`. You don’t need to change anything here — just start everything with:

```docker compose up -d```

If you make changes to *tableflow.properties*, restart Trino to apply them:

```docker compose restart trino```

Trino usually starts quickly. To verify that the container is running and the connection is configured correctly, run a simple SQL command:

```docker exec -it trino trino --execute "SHOW SCHEMAS FROM tableflow"```
At this point, you should see three schemas (for example: information_schema, system, and your Tableflow schema).

You can also run a quick sanity check against your flight data:
```docker exec -it trino trino --execute 'SELECT count(*) AS n FROM tableflow."<CHANGE_TO_YOUR_SCHEMA>".flight_ops_rt'```

Trino is a powerful query engine on its own, but in this workshop we’ll mainly use it behind the scenes to power visualizations in Superset.

### 5.1 Superset configuration

By default, the Superset Docker image does not include Trino support, so we’ll install it manually.

Run the following command to install the required libraries:
```
docker exec -it superset bash -lc "pip install sqlalchemy-trino trino"
```
After the installation completes, restart Superset:
```
docker restart superset
```

Superset needs a bit of time to start. Once it’s ready, use the forwarded port (8080) to open the UI
(right-click the port in Codespaces and choose Open in Browser).

todo add picture

Log in using the default credentials:

Username: admin
Password: admin

Next, go to Settings → Database Connections and create a new database with the following SQLAlchemy URI:
```trino://superset@trino:8080/tableflow```
todo screenshot


Go to datasets Create a dataset
todo add pic








## 📍Step 6. Cleanup

Delete environment
Delete keys
