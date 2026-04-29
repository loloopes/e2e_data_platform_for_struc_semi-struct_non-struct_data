import json
import os
from queue import Empty, Full, Queue
import socket
import threading
import time
import traceback
import warnings
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Optional
from uuid import uuid4

import mlflow
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Consumer, KafkaError




# MLflow (and similar) may define Pydantic fields named `model_name`; silence that noise.
warnings.filterwarnings(
    "ignore",
    message=r'Field "model_name".*protected namespace',
    category=UserWarning,
)

_spark_lock = threading.Lock()
_spark_session = None
_prediction_ddl_done = False
_prediction_hms_registered = False

# ==========================================
# Carregamento do modelo via MLflow Model Registry
# ==========================================

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_MODEL_URI = os.getenv("MLFLOW_MODEL_URI")
RUN_ID = os.getenv("RUN_ID")
MLFLOW_MODEL_ARTIFACT_PATH = os.getenv("MLFLOW_MODEL_ARTIFACT_PATH", "credit_model_pipeline_v2")
MLFLOW_MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "credit_model_pipeline_v2")
MLFLOW_MODEL_STAGE = os.getenv("MLFLOW_MODEL_STAGE", "latest")
SKIP_MODEL_LOAD = os.getenv("SKIP_MODEL_LOAD", "false").lower() in {"1", "true", "yes"}
PREDICTION_LOG_ENABLED = os.getenv("PREDICTION_LOG_ENABLED", "true").lower() in {"1", "true", "yes"}
PREDICTION_LOG_STRICT = os.getenv("PREDICTION_LOG_STRICT", "true").lower() in {"1", "true", "yes"}
PREDICTION_LOG_FLUSH_INTERVAL_MS = int(os.getenv("PREDICTION_LOG_FLUSH_INTERVAL_MS", "200"))
PREDICTION_LOG_BATCH_SIZE = int(os.getenv("PREDICTION_LOG_BATCH_SIZE", "500"))
PREDICTION_LOG_QUEUE_MAXSIZE = int(os.getenv("PREDICTION_LOG_QUEUE_MAXSIZE", "10000"))

# Spark cluster (driver runs in this process; executors on existing workers)
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_HIVE_METASTORE_URIS = os.getenv(
    "SPARK_HIVE_METASTORE_URIS", "thrift://hive-metastore:9083"
)
SPARK_SQL_WAREHOUSE_DIR = os.getenv("SPARK_SQL_WAREHOUSE_DIR", "s3a://lakehouse/")
# Use container_name by default because this API and spark-cluster run in different compose projects.
SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "credit-scoring-api")
SPARK_S3A_ENDPOINT = os.getenv("SPARK_S3A_ENDPOINT", "http://minio:9000")
SPARK_S3A_ACCESS_KEY = os.getenv(
    "SPARK_S3A_ACCESS_KEY", os.getenv("AWS_ACCESS_KEY_ID", "")
)
SPARK_S3A_SECRET_KEY = os.getenv(
    "SPARK_S3A_SECRET_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "")
)
SPARK_PYTHON_BIN = os.getenv("PYSPARK_PYTHON", "python3")
SPARK_DRIVER_PYTHON_BIN = os.getenv("PYSPARK_DRIVER_PYTHON", SPARK_PYTHON_BIN)
SPARK_EXTRA_CLASSPATH = os.getenv(
    "SPARK_EXTRA_CLASSPATH",
    "/opt/extra-jars/hadoop-aws-3.3.4.jar:"
    "/opt/extra-jars/aws-java-sdk-bundle-1.12.262.jar:"
    "/opt/extra-jars/woodstox-core-6.2.8.jar:"
    "/opt/extra-jars/stax2-api-4.2.1.jar:"
    "/opt/extra-jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
)
PREDICTION_LOG_DATABASE = os.getenv("PREDICTION_LOG_DATABASE", "forecast")
PREDICTION_LOG_TABLE = os.getenv("PREDICTION_LOG_TABLE", "prediction_events")
ICEBERG_MAIN_CATALOG = os.getenv("ICEBERG_MAIN_CATALOG", "iceberg")
ICEBERG_HMS_CATALOG = os.getenv("ICEBERG_HMS_CATALOG", "iceberg_hms")
_prediction_queue: "Queue[dict]" = Queue(maxsize=max(PREDICTION_LOG_QUEUE_MAXSIZE, 1))
_prediction_worker_stop = threading.Event()
_prediction_worker_thread: Optional[threading.Thread] = None
_kafka_consumer_stop = threading.Event()
_kafka_consumer_thread: Optional[threading.Thread] = None

KAFKA_CONSUMER_ENABLED = os.getenv("KAFKA_CONSUMER_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
}
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "credit-risk-forecast-consumer")
KAFKA_CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC", "predict")
KAFKA_CONSUMER_SYNC_LAKEHOUSE_WRITE = os.getenv(
    "KAFKA_CONSUMER_SYNC_LAKEHOUSE_WRITE", "true"
).lower() in {"1", "true", "yes"}
KAFKA_CONSUMER_MAX_POLL_RECORDS = max(
    int(os.getenv("KAFKA_CONSUMER_MAX_POLL_RECORDS", "512")),
    1,
)
KAFKA_CONSUMER_BATCH_MAX_WAIT_MS = max(
    int(os.getenv("KAFKA_CONSUMER_BATCH_MAX_WAIT_MS", "200")),
    1,
)
LAKEHOUSE_WRITE_REPARTITION = max(int(os.getenv("LAKEHOUSE_WRITE_REPARTITION", "2")), 0)


def _empty_to_none(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _sql_literal(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _resolve_spark_driver_host(raw_host: str) -> str:
    """
    Spark Standalone rejects underscores in spark:// host URLs.
    Resolve invalid hostnames to a routable IP before building SparkSession.
    """
    candidate = raw_host.strip()
    if not candidate:
        candidate = "127.0.0.1"
    if "_" not in candidate:
        return candidate
    try:
        return socket.gethostbyname(candidate)
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


def _split_model_uris(raw: str) -> list[str]:
    # Allow multiple fallbacks, e.g.:
    # MLFLOW_MODEL_URI="models:/credit_risk_forecast/1,models:/credit_riks_forecast/1"
    parts: list[str] = []
    for chunk in raw.replace(";", ",").split(","):
        uri = chunk.strip()
        if uri:
            parts.append(uri)
    return parts


def _dedupe_preserve_order(uris: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for uri in uris:
        if uri in seen:
            continue
        seen.add(uri)
        out.append(uri)
    return out


def _iter_candidate_model_uris() -> list[str]:
    uris: list[str] = []

    explicit = _empty_to_none(MLFLOW_MODEL_URI)
    if explicit:
        uris.extend(_split_model_uris(explicit))

    run_id = _empty_to_none(RUN_ID)
    if run_id:
        artifact_path = _empty_to_none(MLFLOW_MODEL_ARTIFACT_PATH)
        if artifact_path:
            uris.append(f"runs:/{run_id}/{artifact_path}")

        # If training logged `mlflow.set_tag("logged_model_uri", ...)`, prefer it.
        try:
            from mlflow.tracking import MlflowClient

            run = MlflowClient().get_run(run_id)
            logged_uri = _empty_to_none(run.data.tags.get("logged_model_uri"))
            if logged_uri:
                uris.append(logged_uri)
        except Exception:
            pass

        # Common artifact_path values across this repo.
        for fallback in ("credit_model_pipeline_v2", "model"):
            if fallback != artifact_path:
                uris.append(f"runs:/{run_id}/{fallback}")

    model_name = _empty_to_none(MLFLOW_MODEL_NAME)
    model_stage = _empty_to_none(MLFLOW_MODEL_STAGE)
    if model_name and model_stage:
        uris.append(f"models:/{model_name}/{model_stage}")

    return _dedupe_preserve_order(uris)


def _stop_spark_session() -> None:
    global _spark_session, _prediction_ddl_done, _prediction_hms_registered
    with _spark_lock:
        if _spark_session is not None:
            try:
                _spark_session.stop()
            except Exception as e:
                print(f"Spark stop error: {e}", flush=True)
            _spark_session = None
            _prediction_ddl_done = False
            _prediction_hms_registered = False


def _ensure_spark_session_locked():
    """Create SparkSession if missing. Caller must hold ``_spark_lock``."""
    global _spark_session
    if _spark_session is not None:
        return _spark_session

    from pyspark.sql import SparkSession

    os.makedirs("/tmp/spark-local", exist_ok=True)
    extra_cp = SPARK_EXTRA_CLASSPATH.strip()
    spark_jars = ",".join([item for item in extra_cp.split(":") if item])
    driver_host = _resolve_spark_driver_host(SPARK_DRIVER_HOST)
    builder = (
        SparkSession.builder.appName("credit-scoring-api-lakehouse-log")
        .master(SPARK_MASTER_URL)
        .config("spark.submit.deployMode", "client")
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.warehouse.dir", SPARK_SQL_WAREHOUSE_DIR)
        .config("spark.hadoop.hive.metastore.uris", SPARK_HIVE_METASTORE_URIS)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.fs.s3a.endpoint", SPARK_S3A_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", SPARK_S3A_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SPARK_S3A_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.local.dir", "/tmp/spark-local")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "512m"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "512m"))
        .config("spark.pyspark.python", SPARK_PYTHON_BIN)
        .config("spark.pyspark.driver.python", SPARK_DRIVER_PYTHON_BIN)
        .config("spark.driver.userClassPathFirst", "true")
        .config("spark.executor.userClassPathFirst", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", SPARK_SQL_WAREHOUSE_DIR)
        .config("spark.sql.catalog.iceberg_hms", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_hms.type", "hive")
        .config("spark.sql.catalog.iceberg_hms.uri", SPARK_HIVE_METASTORE_URIS)
        .config("spark.sql.catalog.iceberg_hms.warehouse", SPARK_SQL_WAREHOUSE_DIR)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    if extra_cp:
        builder = (
            builder.config("spark.driver.extraClassPath", extra_cp)
            .config("spark.executor.extraClassPath", extra_cp)
            .config("spark.jars", spark_jars)
        )
    _spark_session = builder.getOrCreate()
    return _spark_session

def _build_prediction_event(
    request_payload: dict[str, Any], response_payload: dict[str, Any], request_id: str
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "request_payload": request_payload,
        "response_payload": response_payload,
    }


def _predict_from_payload(
    application: dict[str, Any],
    request_id: Optional[str] = None,
    sync_log: bool = False,
) -> dict[str, Any]:
    return _predict_batch_from_payloads(
        [application], request_ids=[request_id], sync_log=sync_log
    )[0]


def _predict_batch_from_payloads(
    applications: list[dict[str, Any]],
    request_ids: Optional[list[Optional[str]]] = None,
    sync_log: bool = False,
) -> list[dict[str, Any]]:
    if model is None:
        raise HTTPException(status_code=500, detail="Modelo não carregado no servidor.")
    if not applications:
        return []
    if request_ids is None:
        request_ids = [None] * len(applications)
    if len(request_ids) != len(applications):
        raise HTTPException(
            status_code=400,
            detail="request_ids length must match applications length.",
        )

    request_payloads: list[dict[str, Any]] = []
    resolved_request_ids: list[str] = []
    for application, maybe_request_id in zip(applications, request_ids):
        if not isinstance(application, dict):
            raise HTTPException(status_code=400, detail="Payload deve ser um objeto JSON.")
        normalized_application = _normalize_predict_payload(application)
        request_payloads.append(normalized_application.model_dump())
        resolved_request_ids.append(maybe_request_id or str(uuid4()))

    input_df = pd.DataFrame(request_payloads)
    probabilities = model.predict_proba(input_df)[:, 1]

    responses: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for idx, probability in enumerate(probabilities):
        decision = "Aprovado"
        if probability > 0.5:
            decision = "Negado"
        elif probability > 0.3:
            decision = "Revisão Manual"

        prediction_response = {
            "request_id": resolved_request_ids[idx],
            "probability": round(float(probability), 4),
            "threshold_decision": decision,
            "status": "success",
        }
        event = _build_prediction_event(
            request_payloads[idx],
            prediction_response,
            resolved_request_ids[idx],
        )
        responses.append(prediction_response)
        events.append(event)

    if sync_log:
        _append_prediction_events_to_lakehouse(events)
    else:
        for event in events:
            _enqueue_prediction_event(
                event["request_payload"],
                event["response_payload"],
                event["request_id"],
            )
    return responses


def _decode_kafka_payload(raw_value: Optional[bytes]) -> dict[str, Any]:
    if raw_value is None:
        raise ValueError("Kafka message has empty value.")
    parsed = json.loads(raw_value.decode("utf-8"))
    if isinstance(parsed, dict):
        for key in ("payload", "application", "data", "body"):
            inner = parsed.get(key)
            if isinstance(inner, dict):
                return inner
        return parsed
    raise ValueError("Kafka message value must be a JSON object.")


def _kafka_consumer_worker() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            # Commit offsets only after successful prediction + persistence.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_CONSUMER_TOPIC])
    print(
        "Kafka consumer started "
        f"for topic={KAFKA_CONSUMER_TOPIC} "
        f"sync_lakehouse_write={KAFKA_CONSUMER_SYNC_LAKEHOUSE_WRITE}",
        flush=True,
    )
    pending_msgs = []
    pending_payloads: list[dict[str, Any]] = []
    pending_request_ids: list[Optional[str]] = []
    flush_after_seconds = KAFKA_CONSUMER_BATCH_MAX_WAIT_MS / 1000.0
    next_flush_at = time.monotonic() + flush_after_seconds

    def _flush_batch() -> None:
        nonlocal pending_msgs, pending_payloads, pending_request_ids, next_flush_at
        if not pending_msgs:
            next_flush_at = time.monotonic() + flush_after_seconds
            return
        try:
            predictions = _predict_batch_from_payloads(
                pending_payloads,
                request_ids=pending_request_ids,
                sync_log=KAFKA_CONSUMER_SYNC_LAKEHOUSE_WRITE,
            )
            consumer.commit(asynchronous=False)
            print(
                f"Kafka batch persisted count={len(predictions)} "
                f"last_request_id={predictions[-1]['request_id']}",
                flush=True,
            )
        except Exception as kafka_process_error:
            print(f"Kafka batch processing error: {kafka_process_error}", flush=True)
            print(traceback.format_exc(), flush=True)
            # Fallback to per-message processing to isolate bad records.
            for msg in pending_msgs:
                try:
                    payload = _decode_kafka_payload(msg.value())
                    kafka_request_id = msg.key().decode("utf-8") if msg.key() else None
                    prediction = _predict_from_payload(
                        payload,
                        request_id=kafka_request_id,
                        sync_log=KAFKA_CONSUMER_SYNC_LAKEHOUSE_WRITE,
                    )
                    consumer.commit(message=msg, asynchronous=False)
                    print(
                        f"Kafka prediction persisted request_id={prediction['request_id']}",
                        flush=True,
                    )
                except Exception as single_error:
                    print(f"Kafka processing error: {single_error}", flush=True)
                    print(traceback.format_exc(), flush=True)
        finally:
            pending_msgs = []
            pending_payloads = []
            pending_request_ids = []
            next_flush_at = time.monotonic() + flush_after_seconds

    try:
        while not _kafka_consumer_stop.is_set():
            timeout_seconds = max(0.0, next_flush_at - time.monotonic())
            # Keep short poll timeouts so timed flush can fire predictably.
            msgs = consumer.consume(
                num_messages=KAFKA_CONSUMER_MAX_POLL_RECORDS,
                timeout=min(timeout_seconds, 0.2),
            )

            for msg in msgs:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"Kafka consume error: {msg.error()}", flush=True)
                    continue
                try:
                    pending_payloads.append(_decode_kafka_payload(msg.value()))
                    pending_request_ids.append(msg.key().decode("utf-8") if msg.key() else None)
                    pending_msgs.append(msg)
                except Exception as decode_error:
                    print(f"Kafka decode error: {decode_error}", flush=True)
                    print(traceback.format_exc(), flush=True)

            should_flush_by_size = len(pending_msgs) >= KAFKA_CONSUMER_MAX_POLL_RECORDS
            should_flush_by_time = pending_msgs and time.monotonic() >= next_flush_at
            if should_flush_by_size or should_flush_by_time:
                _flush_batch()
    finally:
        _flush_batch()
        consumer.close()
        print("Kafka consumer stopped.", flush=True)


def _start_kafka_consumer_worker() -> None:
    global _kafka_consumer_thread
    if not KAFKA_CONSUMER_ENABLED:
        return
    if _kafka_consumer_thread and _kafka_consumer_thread.is_alive():
        return
    _kafka_consumer_stop.clear()
    _kafka_consumer_thread = threading.Thread(
        target=_kafka_consumer_worker,
        name="kafka-consumer-worker",
        daemon=True,
    )
    _kafka_consumer_thread.start()


def _stop_kafka_consumer_worker() -> None:
    worker = _kafka_consumer_thread
    if worker is None:
        return
    _kafka_consumer_stop.set()
    worker.join(timeout=5)


def _latest_iceberg_metadata_file(spark, schema: str, table: str) -> str:
    warehouse_root = SPARK_SQL_WAREHOUSE_DIR.rstrip("/")
    metadata_dir = f"{warehouse_root}/{schema}/{table}/metadata"
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(metadata_dir)
    fs = path.getFileSystem(hconf)
    if not fs.exists(path):
        raise RuntimeError(f"Iceberg metadata directory not found: {metadata_dir}")

    files = fs.listStatus(path)
    candidates: list[str] = []
    for file_status in files:
        file_name = file_status.getPath().getName()
        if file_name.endswith(".metadata.json"):
            candidates.append(file_name)
    if not candidates:
        raise RuntimeError(f"No Iceberg metadata file found in {metadata_dir}")

    latest = sorted(candidates)[-1]
    return f"{metadata_dir}/{latest}"


def _ensure_prediction_table_locked(spark) -> None:
    """Idempotent DDL. Caller must hold ``_spark_lock``."""
    global _prediction_ddl_done
    if _prediction_ddl_done:
        return
    db = PREDICTION_LOG_DATABASE
    # Keep both syntaxes for compatibility across catalog implementations.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_MAIN_CATALOG}.{db}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_MAIN_CATALOG}.{db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_HMS_CATALOG}.{db}")
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_HMS_CATALOG}.{db} "
        f"LOCATION '{SPARK_SQL_WAREHOUSE_DIR.rstrip('/')}/{db}'"
    )
    _prediction_ddl_done = True


def _append_prediction_events_to_lakehouse(events: list[dict]) -> None:
    if not PREDICTION_LOG_ENABLED:
        return
    if not events:
        return

    model_name = _empty_to_none(MLFLOW_MODEL_NAME) or ""
    model_stage = _empty_to_none(MLFLOW_MODEL_STAGE) or ""
    rows = []
    for event in events:
        probability_value = event.get("response_payload", {}).get("probability")
        try:
            probability = float(probability_value) if probability_value is not None else None
        except (TypeError, ValueError):
            probability = None
        rows.append(
            {
                "event_id": event["request_id"],
                "event_ts": event["event_ts"],
                "model_name": model_name,
                "model_stage": model_stage,
                "probability": probability,
                "client_id": str(event.get("request_payload", {}).get("id_cliente", "")),
                "request_json": json.dumps(event["request_payload"], ensure_ascii=False),
                "response_json": json.dumps(event["response_payload"], ensure_ascii=False),
            }
        )

    global _prediction_hms_registered
    with _spark_lock:
        from pyspark.sql.utils import AnalysisException

        spark = _ensure_spark_session_locked()
        _ensure_prediction_table_locked(spark)
        table_name = f"{PREDICTION_LOG_DATABASE}.{PREDICTION_LOG_TABLE}"
        # Write through Spark (Iceberg HMS catalog) to persist in lakehouse.
        full_table_name = f"{ICEBERG_HMS_CATALOG}.{table_name}"
        try:
            spark.sql(
                f"ALTER TABLE {full_table_name} "
                "ADD COLUMN IF NOT EXISTS probability DOUBLE"
            )
            spark.sql(
                f"ALTER TABLE {full_table_name} "
                "ADD COLUMN IF NOT EXISTS client_id STRING"
            )
        except Exception:
            # Table may not exist yet; fallback CREATE TABLE path below handles it.
            pass
        event_df = spark.createDataFrame(rows)
        event_df = event_df.select(
            "event_id",
            "event_ts",
            "model_name",
            "model_stage",
            "client_id",
            "probability",
            "request_json",
            "response_json",
        )
        if LAKEHOUSE_WRITE_REPARTITION > 0 and len(rows) > 1:
            event_df = event_df.repartition(LAKEHOUSE_WRITE_REPARTITION)
        writer = (
            event_df.writeTo(full_table_name)
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
        )
        try:
            writer.append()
        except AnalysisException:
            # Some HMS setups do not auto-create namespace via V2 writer path.
            spark.sql(
                f"CREATE DATABASE IF NOT EXISTS {ICEBERG_HMS_CATALOG}.{PREDICTION_LOG_DATABASE}"
            )
            spark.sql(
                f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_HMS_CATALOG}.{PREDICTION_LOG_DATABASE}"
            )
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {full_table_name} ("
                "event_id STRING, "
                "event_ts STRING, "
                "model_name STRING, "
                "model_stage STRING, "
                "client_id STRING, "
                "probability DOUBLE, "
                "request_json STRING, "
                "response_json STRING"
                ") USING iceberg"
            )
            writer.append()

        # Already writing directly to HMS catalog; no manual register needed.
        if not _prediction_hms_registered:
            _prediction_hms_registered = True


def _enqueue_prediction_event(request_payload: dict, response_payload: dict, request_id: str) -> None:
    if not PREDICTION_LOG_ENABLED:
        return
    event = _build_prediction_event(request_payload, response_payload, request_id)
    try:
        _prediction_queue.put_nowait(event)
    except Full as queue_error:
        print("Prediction log queue full. Dropping event.", flush=True)
        if PREDICTION_LOG_STRICT:
            raise RuntimeError("Prediction log queue is full.") from queue_error


def _prediction_log_worker() -> None:
    flush_interval_seconds = max(PREDICTION_LOG_FLUSH_INTERVAL_MS, 1) / 1000.0
    max_batch = max(PREDICTION_LOG_BATCH_SIZE, 1)
    pending: list[dict] = []
    next_flush_at = time.monotonic() + flush_interval_seconds
    while True:
        timeout = max(0.0, next_flush_at - time.monotonic())
        try:
            pending.append(_prediction_queue.get(timeout=timeout))
        except Empty:
            pass

        should_flush = len(pending) >= max_batch or time.monotonic() >= next_flush_at
        should_stop = _prediction_worker_stop.is_set() and _prediction_queue.empty()
        if should_flush and pending:
            try:
                _append_prediction_events_to_lakehouse(pending)
            except Exception as log_error:
                print(f"Prediction batch log error: {log_error}", flush=True)
            finally:
                pending = []
                next_flush_at = time.monotonic() + flush_interval_seconds
        if should_stop and not pending:
            return


def _start_prediction_log_worker() -> None:
    global _prediction_worker_thread
    if not PREDICTION_LOG_ENABLED:
        return
    if _prediction_worker_thread and _prediction_worker_thread.is_alive():
        return
    _prediction_worker_stop.clear()
    _prediction_worker_thread = threading.Thread(
        target=_prediction_log_worker,
        name="prediction-log-worker",
        daemon=True,
    )
    _prediction_worker_thread.start()


def _stop_prediction_log_worker() -> None:
    worker = _prediction_worker_thread
    if worker is None:
        return
    _prediction_worker_stop.set()
    worker.join(timeout=5)


if SKIP_MODEL_LOAD:
    print("SKIP_MODEL_LOAD enabled. Starting API without loading model.", flush=True)
    model = None
else:
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        candidates = _iter_candidate_model_uris()
        if not candidates:
            raise RuntimeError(
                "No model URI candidates resolved. Set MLFLOW_MODEL_URI, "
                "or RUN_ID (+ MLFLOW_MODEL_ARTIFACT_PATH), "
                "or MLFLOW_MODEL_NAME + MLFLOW_MODEL_STAGE."
            )

        last_error: Optional[Exception] = None
        for model_uri in candidates:
            print(f"Loading model from mlflow: {model_uri}...", flush=True)
            try:
                model = mlflow.sklearn.load_model(model_uri=model_uri)
                print("Model loaded successfully!", flush=True)
                last_error = None
                break
            except Exception as e:
                last_error = e
                print(f"Failed loading {model_uri}: {e}", flush=True)

        if last_error is not None:
            raise last_error
    except Exception as e:
        print(f"Error loading model: {e}", flush=True)
        model = None

# ==========================================
# Schema de entrada (payload bruto → DataFrame para o Pipeline)
# ==========================================


class CreditApplication(BaseModel):
    id_cliente: str
    id_contrato: Optional[str] = None
    tipo_contrato: str
    status_contrato: str
    tipo_pagamento: str
    finalidade_emprestimo: str
    tipo_cliente: str
    tipo_portfolio: str
    tipo_produto: str
    categoria_bem: str
    setor_vendedor: str
    canal_venda: str
    faixa_rendimento: Optional[str] = None
    combinacao_produto: Optional[str] = None
    area_venda: Optional[str] = None
    dia_semana_solicitacao: Optional[str] = None
    data_nascimento: str
    data_decisao: str
    data_liberacao: Optional[str] = None
    data_primeiro_vencimento: Optional[str] = None
    data_ultimo_vencimento_original: Optional[str] = None
    data_ultimo_vencimento: Optional[str] = None
    data_encerramento: Optional[str] = None
    valor_solicitado: float
    valor_credito: float
    valor_bem: float
    valor_parcela: float
    valor_entrada: float
    percentual_entrada: float
    qtd_parcelas_planejadas: int
    taxa_juros_padrao: float
    taxa_juros_promocional: float
    hora_solicitacao: int
    flag_ultima_solicitacao_contrato: int
    flag_ultima_solicitacao_dia: int
    acompanhantes_cliente: int
    flag_seguro_contratado: int
    motivo_recusa: Optional[str] = None
    # Cadastral (merge com base_cadastral no treino — opcionais se não enviados)
    renda_anual: Optional[float] = None
    qtd_membros_familia: Optional[int] = None
    possui_carro: Optional[str] = None
    possui_imovel: Optional[str] = None


DEFAULT_PREDICT_PAYLOAD: dict[str, Any] = {
    "id_cliente": "0",
    "id_contrato": None,
    "tipo_contrato": "Cash loans",
    "status_contrato": "Approved",
    "tipo_pagamento": "Cash through a bank",
    "finalidade_emprestimo": "XAP",
    "tipo_cliente": "Repeater",
    "tipo_portfolio": "POS",
    "tipo_produto": "XNA",
    "categoria_bem": "Mobile",
    "setor_vendedor": "Connectivity",
    "canal_venda": "Country-wide",
    "faixa_rendimento": None,
    "combinacao_produto": None,
    "area_venda": None,
    "dia_semana_solicitacao": "Monday",
    "data_nascimento": "1990-01-01",
    "data_decisao": "2024-01-01",
    "data_liberacao": None,
    "data_primeiro_vencimento": None,
    "data_ultimo_vencimento_original": None,
    "data_ultimo_vencimento": None,
    "data_encerramento": None,
    "valor_solicitado": 0.0,
    "valor_credito": 0.0,
    "valor_bem": 0.0,
    "valor_parcela": 0.0,
    "valor_entrada": 0.0,
    "percentual_entrada": 0.0,
    "qtd_parcelas_planejadas": 12,
    "taxa_juros_padrao": 0.03,
    "taxa_juros_promocional": 0.03,
    "hora_solicitacao": 12,
    "flag_ultima_solicitacao_contrato": 0,
    "flag_ultima_solicitacao_dia": 0,
    "acompanhantes_cliente": 0,
    "flag_seguro_contratado": 0,
    "motivo_recusa": None,
    "renda_anual": None,
    "qtd_membros_familia": None,
    "possui_carro": None,
    "possui_imovel": None,
}


def _normalize_predict_payload(payload: dict[str, Any]) -> CreditApplication:
    merged = dict(DEFAULT_PREDICT_PAYLOAD)
    merged.update(payload)
    if not merged.get("id_cliente"):
        merged["id_cliente"] = "0"
    if not merged.get("data_decisao"):
        merged["data_decisao"] = datetime.now(timezone.utc).date().isoformat()
    return CreditApplication.model_validate(merged)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    _start_prediction_log_worker()
    _start_kafka_consumer_worker()
    yield
    _stop_kafka_consumer_worker()
    _stop_prediction_log_worker()
    _stop_spark_session()


app = FastAPI(title="Datarisk Credit Scoring API", lifespan=_lifespan)


@app.post("/predict")
async def predict(application: dict[str, Any]):
    try:
        return _predict_from_payload(application)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "online", "model_loaded": model is not None}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
