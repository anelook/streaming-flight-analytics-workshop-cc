const fs = require("fs");
const csv = require("csv-parser");
const { Kafka, logLevel } = require("kafkajs");
const {
    SchemaRegistry,
    SchemaType,
} = require("@kafkajs/confluent-schema-registry");
require("dotenv").config();

const FILE = "operations.csv";
const MAX_DELAY_MS = 10_000; // cap long gaps; set null to disable
const SPEED = 1; // 2 = 2x faster, 0.5 = slower

// --- Avro schema (value) ---
// logicalType timestamp-millis expects a LONG of epoch millis
const avroSchema = {
    type: "record",
    name: "AdsbExchangeOperationEvent",
    namespace: "com.yourorg.adsb",
    fields: [
        { name: "time", type: { type: "long", logicalType: "timestamp-millis" } },

        { name: "icao", type: "string" },
        {
            name: "operation",
            type: {
                type: "enum",
                name: "OperationType",
                symbols: ["landing", "takeoff", "unknown"],
            },
            default: "unknown",
        },

        { name: "airport", type: ["null", "string"], default: null },
        { name: "registration", type: ["null", "string"], default: null },
        { name: "flight", type: ["null", "string"], default: null },

        { name: "ac_type", type: ["null", "string"], default: null },
        { name: "runway", type: ["null", "string"], default: null },

        { name: "flight_link", type: ["null", "string"], default: null },
        { name: "squawk", type: ["null", "string"], default: null },

        { name: "signal_type", type: ["null", "string"], default: null },
        { name: "category", type: ["null", "string"], default: null },

        { name: "year", type: ["null", "int"], default: null },
        { name: "manufacturer", type: ["null", "string"], default: null },
        { name: "model", type: ["null", "string"], default: null },
        { name: "ownop", type: ["null", "string"], default: null },

        { name: "faa_pia", type: ["null", "boolean"], default: null },
        { name: "faa_ladd", type: ["null", "boolean"], default: null },

        { name: "short_type", type: ["null", "string"], default: null },
        { name: "mil", type: ["null", "boolean"], default: null },

        { name: "apt_type", type: ["null", "string"], default: null },
        { name: "name", type: ["null", "string"], default: null },

        { name: "continent", type: ["null", "string"], default: null },
        { name: "iso_country", type: ["null", "string"], default: null },
        { name: "iso_region", type: ["null", "string"], default: null },
        { name: "municipality", type: ["null", "string"], default: null },

        { name: "scheduled_service", type: ["null", "boolean"], default: null },
        { name: "iata_code", type: ["null", "string"], default: null },

        { name: "elev", type: ["null", "int"], default: null }
    ],
};

// --- Helpers ---
function parseCsvTimeToMs(s) {
    // Input like "2025-11-01 00:00:04"
    // Treat as UTC for simplicity. If you want local, remove the trailing "Z".
    const iso = String(s).replace(" ", "T") + "Z";
    const ms = Date.parse(iso);
    return Number.isNaN(ms) ? null : ms;
}

function msSinceMidnight(d) {
    return (
        (d.getHours() * 3600 + d.getMinutes() * 60 + d.getSeconds()) * 1000 +
        d.getMilliseconds()
    );
}

const toNullableString = (v) => {
    if (v == null) return null;
    const s = String(v).trim();
    return s === "" ? null : s;
};

const toIntOrNull = (v) => {
    const s = toNullableString(v);
    if (s == null) return null;
    const n = Number.parseInt(s, 10);
    return Number.isNaN(n) ? null : n;
};

const tfToBoolOrNull = (v) => {
    const s = toNullableString(v);
    if (s == null) return null;
    const x = s.toLowerCase();
    if (x === "t" || x === "true") return true;
    if (x === "f" || x === "false") return false;
    return null;
};

const yesNoToBoolOrNull = (v) => {
    const s = toNullableString(v);
    if (s == null) return null;
    const x = s.toLowerCase();
    if (x === "yes") return true;
    if (x === "no") return false;
    return null;
};

function normalizeOperation(op) {
    const s = toNullableString(op);
    if (!s) return "unknown";
    const x = s.toLowerCase();
    if (x === "landing") return "landing";
    if (x === "takeoff") return "takeoff";
    return "unknown";
}

function toAvroPayloadFromCsvRow(row, nowMs) {
    // row is strings from csv-parser
    return {
        time: nowMs, // timestamp-millis long
        icao: String(row.icao || "").trim(), // required string
        operation: normalizeOperation(row.operation),

        airport: toNullableString(row.airport),
        registration: toNullableString(row.registration),
        flight: toNullableString(row.flight),

        ac_type: toNullableString(row.ac_type),
        runway: toNullableString(row.runway),

        flight_link: toNullableString(row.flight_link),
        squawk: toNullableString(row.squawk),

        signal_type: toNullableString(row.signal_type),
        category: toNullableString(row.category),

        year: toIntOrNull(row.year),
        manufacturer: toNullableString(row.manufacturer),
        model: toNullableString(row.model),
        ownop: toNullableString(row.ownop),

        faa_pia: tfToBoolOrNull(row.faa_pia),
        faa_ladd: tfToBoolOrNull(row.faa_ladd),

        short_type: toNullableString(row.short_type),
        mil: tfToBoolOrNull(row.mil),

        apt_type: toNullableString(row.apt_type),
        name: toNullableString(row.name),

        continent: toNullableString(row.continent),
        iso_country: toNullableString(row.iso_country),
        iso_region: toNullableString(row.iso_region),
        municipality: toNullableString(row.municipality),

        scheduled_service: yesNoToBoolOrNull(row.scheduled_service),
        iata_code: toNullableString(row.iata_code),

        elev: toIntOrNull(row.elev)
    };
}

async function main() {
    // --- Kafka env ---
    const brokers = (process.env.KAFKA_BROKERS || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    const kafkaUsername = process.env.KAFKA_USERNAME;
    const kafkaPassword = process.env.KAFKA_PASSWORD;
    const topic = process.env.KAFKA_TOPIC;

    // --- Schema Registry env ---
    const srHost = process.env.SCHEMA_REGISTRY_URL;
    const srUsername = process.env.SCHEMA_REGISTRY_USERNAME;
    const srPassword = process.env.SCHEMA_REGISTRY_PASSWORD;

    if (!brokers.length || !kafkaUsername || !kafkaPassword || !topic) {
        throw new Error(
            "Missing Kafka env vars: KAFKA_BROKERS, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_TOPIC"
        );
    }
    if (!srHost || !srUsername || !srPassword) {
        throw new Error(
            "Missing Schema Registry env vars: SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_USERNAME, SCHEMA_REGISTRY_PASSWORD"
        );
    }

    // KafkaJS client
    const kafka = new Kafka({
        clientId: "adsb-ops-replayer-avro",
        brokers,
        ssl: true,
        sasl: { mechanism: "plain", username: kafkaUsername, password: kafkaPassword },
        logLevel: logLevel.NOTHING,
    });

    const producer = kafka.producer({ retry: { retries: 10 } });

    // Schema Registry client
    const registry = new SchemaRegistry({
        host: srHost,
        auth: { username: srUsername, password: srPassword },
    });

    // Confluent default subject naming is usually "<topic>-value"
    const subject = `${topic}-value`;

    // Register schema (idempotent if same schema already exists under subject)
    const { id: schemaId } = await registry.register(
        { type: SchemaType.AVRO, schema: JSON.stringify(avroSchema) },
        { subject }
    );

    await producer.connect();
    console.log("Kafka producer connected. Schema id:", schemaId, "subject:", subject);

    const shutdown = async () => {
        try {
            console.log("Shutting down...");
            await producer.disconnect();
        } finally {
            process.exit(0);
        }
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    // --- Replay logic ---
    let started = false;
    let prevRowTimeMs = null;

    const now = new Date();
    const nowTodMs = msSinceMidnight(now);
    console.log("Now local time:", now.toString());

    const stream = fs.createReadStream(FILE).pipe(csv());

    stream.on("data", (row) => {
        const rowTimeMs = parseCsvTimeToMs(row.time);
        if (rowTimeMs == null) return;

        const rowTodMs = msSinceMidnight(new Date(rowTimeMs));

        // Start at the current time-of-day in the file
        if (!started) {
            if (rowTodMs < nowTodMs) return;

            started = true;
            prevRowTimeMs = rowTimeMs;

            stream.pause();
            void produceRowAvro(producer, registry, schemaId, topic, row).finally(() =>
                stream.resume()
            );
            return;
        }

        const deltaMs = Math.max(0, rowTimeMs - prevRowTimeMs);
        prevRowTimeMs = rowTimeMs;

        let delay = deltaMs / SPEED;
        if (MAX_DELAY_MS != null) delay = Math.min(delay, MAX_DELAY_MS);

        stream.pause();
        setTimeout(() => {
            void produceRowAvro(producer, registry, schemaId, topic, row).finally(() =>
                stream.resume()
            );
        }, delay);
    });

    stream.on("end", async () => {
        console.log("Replay finished (EOF). Flushing...");
        try {
            await producer.flush();
        } catch {}
        await producer.disconnect();
        console.log("Done.");
    });

    stream.on("error", async (err) => {
        console.error("Stream error:", err);
        try {
            await producer.disconnect();
        } catch {}
        process.exit(1);
    });
}

async function produceRowAvro(producer, registry, schemaId, topic, row) {
    const nowMs = Date.now();

    const payload = toAvroPayloadFromCsvRow(row, nowMs);

    // Encode using Confluent wire format (magic byte + schema id + avro payload)
    const value = await registry.encode(schemaId, payload);
    
    const key = row.icao ? String(row.icao).trim() : undefined;

    await producer.send({
        topic,
        messages: [{ key, value }],
    });
}

main().catch((e) => {
    console.error("Fatal:", e);
    process.exit(1);
});
