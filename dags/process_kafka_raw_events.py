import json
import logging

import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


LOGGER = logging.getLogger(__name__)

SUPPORTED_TRANSACTION_EVENTS = {
    "TRANSACTION_CREATED",
}

SUPPORTED_PAYMENT_EVENTS = {
    "PAYMENT_CREATED",
    "PAYMENT_COMPLETED",
}

SUPPORTED_CREDIT_EVENTS = {
    "CREDIT_REQUESTED",
    "CREDIT_APPROVED",
}


def get_event_data(payload, object_name):
    """
    Returns the business object stored under payload.data.
    """

    if isinstance(payload, str):
        payload = json.loads(payload)

    if not isinstance(payload, dict):
        raise ValueError(
            "Payload must be a valid JSON object"
        )

    event_data = payload.get("data")

    if not isinstance(event_data, dict):
        raise ValueError(
            f"payload.data must contain the {object_name} object"
        )

    event_timestamp = payload.get("event_timestamp")

    if not event_timestamp:
        raise ValueError(
            "event_timestamp is missing"
        )

    return payload, event_data, event_timestamp


def validate_required_fields(
    event_data,
    required_fields,
    object_name,
):
    """
    Checks whether all required business fields exist.
    """

    missing_fields = [
        field
        for field in required_fields
        if event_data.get(field) is None
    ]

    if missing_fields:
        raise ValueError(
            f"Missing {object_name} fields: "
            + ", ".join(missing_fields)
        )


def process_transaction_event(
    cursor,
    payload,
    kafka_topic,
    partition_id,
    kafka_offset,
):
    """
    Loads one transaction event into stage.transactions.
    """

    _, transaction, event_timestamp = get_event_data(
        payload=payload,
        object_name="transaction",
    )

    validate_required_fields(
        event_data=transaction,
        required_fields=[
            "transaction_id",
            "account_id",
            "transaction_type",
            "amount",
            "currency",
            "status",
            "booking_date",
        ],
        object_name="transaction",
    )

    cursor.execute(
        """
        INSERT INTO stage.transactions
        (
            transaction_id,
            account_id,
            transaction_type,
            amount,
            currency,
            status,
            booking_date,
            event_timestamp,
            kafka_topic,
            partition_id,
            kafka_offset
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (transaction_id)
        DO UPDATE SET
            account_id =
                EXCLUDED.account_id,
            transaction_type =
                EXCLUDED.transaction_type,
            amount =
                EXCLUDED.amount,
            currency =
                EXCLUDED.currency,
            status =
                EXCLUDED.status,
            booking_date =
                EXCLUDED.booking_date,
            event_timestamp =
                EXCLUDED.event_timestamp,
            kafka_topic =
                EXCLUDED.kafka_topic,
            partition_id =
                EXCLUDED.partition_id,
            kafka_offset =
                EXCLUDED.kafka_offset,
            loaded_at =
                CURRENT_TIMESTAMP;
        """,
        (
            int(transaction["transaction_id"]),
            int(transaction["account_id"]),
            transaction["transaction_type"],
            transaction["amount"],
            transaction["currency"],
            transaction["status"],
            transaction["booking_date"],
            event_timestamp,
            kafka_topic,
            partition_id,
            kafka_offset,
        ),
    )


def process_payment_event(
    cursor,
    payload,
    kafka_topic,
    partition_id,
    kafka_offset,
):
    """
    Loads one payment event into stage.payments.
    """

    _, payment, event_timestamp = get_event_data(
        payload=payload,
        object_name="payment",
    )

    validate_required_fields(
        event_data=payment,
        required_fields=[
            "payment_id",
            "debtor_account",
            "creditor_account",
            "payment_method",
            "amount",
            "currency",
            "status",
            "execution_date",
        ],
        object_name="payment",
    )

    cursor.execute(
        """
        INSERT INTO stage.payments
        (
            payment_id,
            debtor_account,
            creditor_account,
            payment_method,
            amount,
            currency,
            status,
            execution_date,
            event_timestamp,
            kafka_topic,
            partition_id,
            kafka_offset
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (payment_id)
        DO UPDATE SET
            debtor_account =
                EXCLUDED.debtor_account,
            creditor_account =
                EXCLUDED.creditor_account,
            payment_method =
                EXCLUDED.payment_method,
            amount =
                EXCLUDED.amount,
            currency =
                EXCLUDED.currency,
            status =
                EXCLUDED.status,
            execution_date =
                EXCLUDED.execution_date,
            event_timestamp =
                EXCLUDED.event_timestamp,
            kafka_topic =
                EXCLUDED.kafka_topic,
            partition_id =
                EXCLUDED.partition_id,
            kafka_offset =
                EXCLUDED.kafka_offset,
            loaded_at =
                CURRENT_TIMESTAMP;
        """,
        (
            int(payment["payment_id"]),
            int(payment["debtor_account"]),
            int(payment["creditor_account"]),
            payment["payment_method"],
            payment["amount"],
            payment["currency"],
            payment["status"],
            payment["execution_date"],
            event_timestamp,
            kafka_topic,
            partition_id,
            kafka_offset,
        ),
    )


def process_credit_event(
    cursor,
    payload,
    kafka_topic,
    partition_id,
    kafka_offset,
):
    """
    Loads one credit event into stage.credit.
    """

    _, credit, event_timestamp = get_event_data(
        payload=payload,
        object_name="credit",
    )

    validate_required_fields(
        event_data=credit,
        required_fields=[
            "credit_id",
            "customer_id",
            "credit_type",
            "requested_amount",
            "currency",
            "status",
        ],
        object_name="credit",
    )

    cursor.execute(
        """
        INSERT INTO stage.credit
        (
            credit_id,
            customer_id,
            credit_type,
            requested_amount,
            approved_amount,
            currency,
            interest_rate,
            term_months,
            status,
            event_timestamp,
            kafka_topic,
            partition_id,
            kafka_offset
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (credit_id)
        DO UPDATE SET
            customer_id =
                EXCLUDED.customer_id,
            credit_type =
                EXCLUDED.credit_type,
            requested_amount =
                EXCLUDED.requested_amount,
            approved_amount =
                EXCLUDED.approved_amount,
            currency =
                EXCLUDED.currency,
            interest_rate =
                EXCLUDED.interest_rate,
            term_months =
                EXCLUDED.term_months,
            status =
                EXCLUDED.status,
            event_timestamp =
                EXCLUDED.event_timestamp,
            kafka_topic =
                EXCLUDED.kafka_topic,
            partition_id =
                EXCLUDED.partition_id,
            kafka_offset =
                EXCLUDED.kafka_offset,
            loaded_at =
                CURRENT_TIMESTAMP;
        """,
        (
            int(credit["credit_id"]),
            int(credit["customer_id"]),
            credit["credit_type"],
            credit["requested_amount"],
            credit.get("approved_amount"),
            credit["currency"],
            credit.get("interest_rate"),
            credit.get("term_months"),
            credit["status"],
            event_timestamp,
            kafka_topic,
            partition_id,
            kafka_offset,
        ),
    )


@dag(
    dag_id="process_kafka_raw_events",
    description=(
        "Process NEW Kafka events and load Stage tables"
    ),
    schedule="*/1 * * * *",
    start_date=pendulum.datetime(
        2026,
        9,
        14,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "kafka",
        "postgresql",
        "stage",
        "etl",
    ],
)
def process_kafka_raw_events_dag():

    @task(
        task_id="process_new_events",
        retries=2,
        retry_delay=pendulum.duration(seconds=20),
    )
    def process_new_events():

        hook = PostgresHook(
            postgres_conn_id="postgres_default",
            database="kafka_events",
        )

        connection = hook.get_conn()

        processed_count = 0
        error_count = 0

        try:
            connection.autocommit = False

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        id,
                        event_id,
                        event_type,
                        payload,
                        kafka_topic,
                        partition_id,
                        kafka_offset
                    FROM public.raw_events
                    WHERE processing_status = 'NEW'
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 100;
                    """
                )

                events = cursor.fetchall()

                LOGGER.info(
                    "Found %s NEW event(s)",
                    len(events),
                )

                for event in events:
                    (
                        raw_id,
                        event_id,
                        event_type,
                        payload,
                        kafka_topic,
                        partition_id,
                        kafka_offset,
                    ) = event

                    cursor.execute(
                        "SAVEPOINT process_single_event"
                    )

                    try:
                        if not event_id:
                            raise ValueError(
                                "event_id is missing"
                            )

                        if not event_type:
                            raise ValueError(
                                "event_type is missing"
                            )

                        if (
                            event_type
                            in SUPPORTED_TRANSACTION_EVENTS
                        ):
                            process_transaction_event(
                                cursor=cursor,
                                payload=payload,
                                kafka_topic=kafka_topic,
                                partition_id=partition_id,
                                kafka_offset=kafka_offset,
                            )

                        elif (
                            event_type
                            in SUPPORTED_PAYMENT_EVENTS
                        ):
                            process_payment_event(
                                cursor=cursor,
                                payload=payload,
                                kafka_topic=kafka_topic,
                                partition_id=partition_id,
                                kafka_offset=kafka_offset,
                            )

                        elif (
                            event_type
                            in SUPPORTED_CREDIT_EVENTS
                        ):
                            process_credit_event(
                                cursor=cursor,
                                payload=payload,
                                kafka_topic=kafka_topic,
                                partition_id=partition_id,
                                kafka_offset=kafka_offset,
                            )

                        else:
                            raise ValueError(
                                "Unsupported event type: "
                                f"{event_type}"
                            )

                        cursor.execute(
                            """
                            UPDATE public.raw_events
                            SET
                                processing_status =
                                    'PROCESSED',
                                processed_at =
                                    CURRENT_TIMESTAMP,
                                error_message = NULL
                            WHERE id = %s;
                            """,
                            (raw_id,),
                        )

                        cursor.execute(
                            "RELEASE SAVEPOINT "
                            "process_single_event"
                        )

                        processed_count += 1

                        LOGGER.info(
                            "Event processed: "
                            "raw_id=%s, "
                            "event_id=%s, "
                            "event_type=%s, "
                            "topic=%s",
                            raw_id,
                            event_id,
                            event_type,
                            kafka_topic,
                        )

                    except Exception as event_error:
                        cursor.execute(
                            "ROLLBACK TO SAVEPOINT "
                            "process_single_event"
                        )

                        cursor.execute(
                            """
                            UPDATE public.raw_events
                            SET
                                processing_status = 'ERROR',
                                processed_at =
                                    CURRENT_TIMESTAMP,
                                error_message = %s
                            WHERE id = %s;
                            """,
                            (
                                str(event_error)[:2000],
                                raw_id,
                            ),
                        )

                        cursor.execute(
                            "RELEASE SAVEPOINT "
                            "process_single_event"
                        )

                        error_count += 1

                        LOGGER.exception(
                            "Event processing failed: "
                            "raw_id=%s, "
                            "event_id=%s, "
                            "event_type=%s",
                            raw_id,
                            event_id,
                            event_type,
                        )

            connection.commit()

            result = {
                "processed": processed_count,
                "errors": error_count,
                "total": processed_count + error_count,
            }

            LOGGER.info(
                "Processing result: %s",
                result,
            )

            return result

        except Exception:
            connection.rollback()

            LOGGER.exception(
                "DAG processing transaction failed"
            )

            raise

        finally:
            connection.close()

    process_new_events()


process_kafka_raw_events_dag()