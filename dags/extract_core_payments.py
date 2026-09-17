import json
import logging
from datetime import date, datetime
from decimal import Decimal

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kafka import KafkaProducer


LOGGER = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_default"

KAFKA_SERVERS = [
    "broker-1:19092",
    "broker-2:19092",
    "broker-3:19092",
]

KAFKA_TOPIC = "core.payments"


def json_serializer(value):
    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    raise TypeError(
        f"Type {type(value).__name__} is not JSON serializable"
    )


@dag(
    dag_id="extract_core_payments",
    description=(
        "Extract new Core Banking payments "
        "and publish them to Kafka"
    ),
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(2026, 9, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["core-banking", "kafka", "payments"],
)
def extract_core_payments_dag():

    @task(
        task_id="publish_new_payments",
        retries=2,
        retry_delay=pendulum.duration(seconds=20),
    )
    def extract_and_publish_payments():

        postgres_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
            database="kafka_events",
        )

        connection = postgres_hook.get_conn()
        producer = None

        try:
            connection.autocommit = False

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT last_processed_id
                    FROM control.pipeline_watermark
                    WHERE source_schema = 'core'
                      AND source_table = 'payments'
                    FOR UPDATE;
                    """
                )

                watermark_row = cursor.fetchone()

                if watermark_row is None:
                    raise ValueError(
                        "Watermark for core.payments was not found"
                    )

                last_processed_id = watermark_row[0]

                LOGGER.info(
                    "Current payments watermark: %s",
                    last_processed_id,
                )

                cursor.execute(
                    """
                    SELECT
                        payment_id,
                        debtor_account,
                        creditor_account,
                        payment_method,
                        amount,
                        currency,
                        status,
                        execution_date,
                        created_at,
                        updated_at
                    FROM core.payments
                    WHERE payment_id > %s
                    ORDER BY payment_id;
                    """,
                    (last_processed_id,),
                )

                columns = [
                    description[0]
                    for description in cursor.description
                ]

                rows = cursor.fetchall()

                LOGGER.info(
                    "Found %s new payment(s)",
                    len(rows),
                )

                if not rows:
                    connection.rollback()

                    result = {
                        "published": 0,
                        "previous_watermark": last_processed_id,
                        "new_watermark": last_processed_id,
                    }

                    LOGGER.info(
                        "No new payments found: %s",
                        result,
                    )

                    return result

                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_SERVERS,
                    acks="all",
                    retries=5,
                    enable_idempotence=True,
                    key_serializer=lambda key: str(key).encode(
                        "utf-8"
                    ),
                    value_serializer=lambda value: json.dumps(
                        value,
                        default=json_serializer,
                        ensure_ascii=False,
                    ).encode("utf-8"),
                )

                highest_payment_id = last_processed_id
                delivery_futures = []

                for row in rows:
                    payment = dict(zip(columns, row))

                    if payment["status"] == "COMPLETED":
                        event_type = "PAYMENT_COMPLETED"
                    else:
                        event_type = "PAYMENT_CREATED"

                    message = {
                        "event_id": payment["payment_id"],
                        "event_type": event_type,
                        "source_system": "CORE_BANKING",
                        "source_schema": "core",
                        "source_table": "payments",
                        "event_timestamp": payment["created_at"],
                        "data": payment,
                    }

                    delivery_future = producer.send(
                        topic=KAFKA_TOPIC,
                        key=payment["debtor_account"],
                        value=message,
                    )

                    delivery_futures.append(
                        (
                            payment["payment_id"],
                            delivery_future,
                        )
                    )

                    highest_payment_id = max(
                        highest_payment_id,
                        payment["payment_id"],
                    )

                for payment_id, delivery_future in delivery_futures:
                    metadata = delivery_future.get(timeout=30)

                    LOGGER.info(
                        "Payment published: "
                        "payment_id=%s, topic=%s, "
                        "partition=%s, offset=%s",
                        payment_id,
                        metadata.topic,
                        metadata.partition,
                        metadata.offset,
                    )

                producer.flush()

                cursor.execute(
                    """
                    UPDATE control.pipeline_watermark
                    SET
                        last_processed_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE source_schema = 'core'
                      AND source_table = 'payments';
                    """,
                    (highest_payment_id,),
                )

                connection.commit()

                result = {
                    "published": len(rows),
                    "previous_watermark": last_processed_id,
                    "new_watermark": highest_payment_id,
                }

                LOGGER.info(
                    "Payment extraction completed: %s",
                    result,
                )

                return result

        except Exception:
            connection.rollback()

            LOGGER.exception(
                "Payment extraction and publishing failed"
            )

            raise

        finally:
            if producer is not None:
                producer.close()

            connection.close()

    extract_and_publish_payments()


extract_core_payments_dag()