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
KAFKA_TOPIC = "core.transactions"


def json_serializer(value):
    """
    Converts PostgreSQL-specific Python types into values
    that can be serialized as JSON.
    """

    # Keep monetary values precise.
    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    raise TypeError(
        f"Type {type(value).__name__} is not JSON serializable"
    )


@dag(
    dag_id="extract_core_transactions",
    description=(
        "Extract new Core Banking transactions "
        "and publish them to Kafka"
    ),
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(2026, 9, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["core-banking", "kafka", "transactions"],
)
def extract_core_transactions_dag():

    @task(
        task_id="publish_new_transactions",
        retries=2,
        retry_delay=pendulum.duration(seconds=20),
    )
    def extract_and_publish_transactions():
        """
        Reads transactions newer than the current watermark,
        publishes them to Kafka, and advances the watermark
        only after successful delivery.
        """

        postgres_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
            database="kafka_events",
        )

        connection = postgres_hook.get_conn()
        producer = None

        try:
            connection.autocommit = False

            with connection.cursor() as cursor:
                # Lock the watermark row to prevent two concurrent
                # executions from extracting the same records.
                cursor.execute(
                    """
                    SELECT last_processed_id
                    FROM control.pipeline_watermark
                    WHERE source_schema = 'core'
                      AND source_table = 'transactions'
                    FOR UPDATE;
                    """
                )

                watermark_row = cursor.fetchone()

                if watermark_row is None:
                    raise ValueError(
                        "Watermark for core.transactions was not found"
                    )

                last_processed_id = watermark_row[0]

                LOGGER.info(
                    "Current transactions watermark: %s",
                    last_processed_id,
                )

                cursor.execute(
                    """
                    SELECT
                        transaction_id,
                        account_id,
                        transaction_type,
                        amount,
                        currency,
                        status,
                        booking_date,
                        created_at,
                        updated_at
                    FROM core.transactions
                    WHERE transaction_id > %s
                    ORDER BY transaction_id;
                    """,
                    (last_processed_id,),
                )

                columns = [
                    description[0]
                    for description in cursor.description
                ]

                rows = cursor.fetchall()

                LOGGER.info(
                    "Found %s new transaction(s)",
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
                        "No new transactions found: %s",
                        result,
                    )

                    return result

                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_SERVERS,
                    acks="all",
                    retries=5,
                    enable_idempotence=True,
                    key_serializer=lambda key: str(key).encode("utf-8"),
                    value_serializer=lambda value: json.dumps(
                        value,
                        default=json_serializer,
                        ensure_ascii=False,
                    ).encode("utf-8"),
                )

                highest_transaction_id = last_processed_id
                delivery_futures = []

                for row in rows:
                    transaction = dict(zip(columns, row))

                    message = {
                        "event_id": transaction["transaction_id"],
                        "event_type": "TRANSACTION_CREATED",
                        "source_system": "CORE_BANKING",
                        "source_schema": "core",
                        "source_table": "transactions",
                        "event_timestamp": transaction["created_at"],
                        "data": transaction,
                    }

                    delivery_future = producer.send(
                        topic=KAFKA_TOPIC,
                        key=transaction["account_id"],
                        value=message,
                    )

                    delivery_futures.append(
                        (
                            transaction["transaction_id"],
                            delivery_future,
                        )
                    )

                    highest_transaction_id = max(
                        highest_transaction_id,
                        transaction["transaction_id"],
                    )

                # Wait for Kafka to acknowledge every message.
                for transaction_id, delivery_future in delivery_futures:
                    metadata = delivery_future.get(timeout=30)

                    LOGGER.info(
                        "Transaction published: "
                        "transaction_id=%s, topic=%s, "
                        "partition=%s, offset=%s",
                        transaction_id,
                        metadata.topic,
                        metadata.partition,
                        metadata.offset,
                    )

                producer.flush()

                # Advance the watermark only after all messages
                # have been acknowledged by Kafka.
                cursor.execute(
                    """
                    UPDATE control.pipeline_watermark
                    SET
                        last_processed_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE source_schema = 'core'
                      AND source_table = 'transactions';
                    """,
                    (highest_transaction_id,),
                )

                connection.commit()

                result = {
                    "published": len(rows),
                    "previous_watermark": last_processed_id,
                    "new_watermark": highest_transaction_id,
                }

                LOGGER.info(
                    "Transaction extraction completed: %s",
                    result,
                )

                return result

        except Exception:
            connection.rollback()

            LOGGER.exception(
                "Transaction extraction and publishing failed"
            )

            raise

        finally:
            if producer is not None:
                producer.close()

            connection.close()

    extract_and_publish_transactions()


extract_core_transactions_dag()