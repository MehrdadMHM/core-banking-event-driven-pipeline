"""Generate synthetic Core Banking activity for the local Kafka demo.

This simulator writes only to the three operational source tables:
    core.transactions
    core.payments
    core.credit

Airflow is responsible for extracting the new rows and publishing events.
All generated data is fictional and must not be used as real banking data.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal

import psycopg2
from psycopg2.extensions import connection as PgConnection


LOGGER = logging.getLogger("core-banking-simulator")
STOP_REQUESTED = False


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_environment(cls) -> "DatabaseConfig":
        return cls(
            host=os.getenv("CORE_DB_HOST", "localhost"),
            port=int(os.getenv("CORE_DB_PORT", "5432")),
            database=os.getenv("CORE_DB_NAME", "kafka_events"),
            user=os.getenv("CORE_DB_USER", "airflow"),
            password=os.getenv("CORE_DB_PASSWORD", "airflow"),
        )


ACCOUNT_IDS = list(range(1_001_001, 1_001_101))
CUSTOMER_IDS = list(range(500_001, 500_101))


def money(minimum: int, maximum: int) -> Decimal:
    """Return a positive monetary value with exactly two decimals."""

    cents = random.randint(minimum * 100, maximum * 100)
    return (Decimal(cents) / Decimal("100")).quantize(Decimal("0.01"))


def connect(config: DatabaseConfig) -> PgConnection:
    LOGGER.info(
        "Connecting to PostgreSQL at %s:%s/%s",
        config.host,
        config.port,
        config.database,
    )
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=10,
        application_name="core_banking_simulator",
    )


def insert_transaction(connection: PgConnection) -> int:
    transaction_type = random.choices(
        ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "FEE"],
        weights=[15, 20, 60, 5],
        k=1,
    )[0]
    status = random.choices(
        ["COMPLETED", "PENDING", "REJECTED", "CANCELLED"],
        weights=[85, 10, 4, 1],
        k=1,
    )[0]

    if transaction_type == "FEE":
        amount = money(1, 50)
    elif transaction_type == "WITHDRAWAL":
        amount = money(10, 2_000)
    else:
        amount = money(10, 10_000)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO core.transactions
            (
                account_id,
                transaction_type,
                amount,
                currency,
                status
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING transaction_id;
            """,
            (
                random.choice(ACCOUNT_IDS),
                transaction_type,
                amount,
                "EUR",
                status,
            ),
        )
        transaction_id = cursor.fetchone()[0]

    connection.commit()
    LOGGER.info(
        "TRANSACTION created: id=%s type=%s amount=%s EUR status=%s",
        transaction_id,
        transaction_type,
        amount,
        status,
    )
    return transaction_id


def insert_payment(connection: PgConnection) -> int:
    debtor_account = random.choice(ACCOUNT_IDS)
    creditor_account = random.choice(ACCOUNT_IDS)
    while creditor_account == debtor_account:
        creditor_account = random.choice(ACCOUNT_IDS)

    payment_method = random.choices(
        ["SEPA", "INSTANT", "CARD", "DIRECT_DEBIT"],
        weights=[45, 20, 20, 15],
        k=1,
    )[0]
    status = random.choices(
        ["CREATED", "PROCESSING", "COMPLETED", "FAILED", "CANCELLED"],
        weights=[10, 10, 73, 5, 2],
        k=1,
    )[0]
    amount = money(1, 7_500)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO core.payments
            (
                debtor_account,
                creditor_account,
                payment_method,
                amount,
                currency,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING payment_id;
            """,
            (
                debtor_account,
                creditor_account,
                payment_method,
                amount,
                "EUR",
                status,
            ),
        )
        payment_id = cursor.fetchone()[0]

    connection.commit()
    LOGGER.info(
        "PAYMENT created: id=%s method=%s amount=%s EUR status=%s",
        payment_id,
        payment_method,
        amount,
        status,
    )
    return payment_id


def insert_credit(connection: PgConnection) -> int:
    credit_type = random.choices(
        ["PERSONAL", "MORTGAGE", "CAR", "OVERDRAFT"],
        weights=[40, 20, 25, 15],
        k=1,
    )[0]
    status = random.choices(
        ["REQUESTED", "UNDER_REVIEW", "APPROVED", "REJECTED", "ACTIVE"],
        weights=[20, 25, 20, 10, 25],
        k=1,
    )[0]

    ranges = {
        "PERSONAL": (2_000, 50_000, (12, 84)),
        "MORTGAGE": (100_000, 750_000, (120, 360)),
        "CAR": (10_000, 100_000, (24, 84)),
        "OVERDRAFT": (500, 25_000, (12, 24)),
    }
    minimum, maximum, term_range = ranges[credit_type]
    requested_amount = money(minimum, maximum)
    term_months = random.randint(*term_range)

    approved_amount = None
    interest_rate = None
    if status in {"APPROVED", "ACTIVE"}:
        approval_ratio = Decimal(random.randint(75, 100)) / Decimal("100")
        approved_amount = (requested_amount * approval_ratio).quantize(
            Decimal("0.01")
        )
        interest_rate = (
            Decimal(random.randint(250, 950)) / Decimal("100")
        ).quantize(Decimal("0.01"))

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO core.credit
            (
                customer_id,
                credit_type,
                requested_amount,
                approved_amount,
                currency,
                interest_rate,
                term_months,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING credit_id;
            """,
            (
                random.choice(CUSTOMER_IDS),
                credit_type,
                requested_amount,
                approved_amount,
                "EUR",
                interest_rate,
                term_months,
                status,
            ),
        )
        credit_id = cursor.fetchone()[0]

    connection.commit()
    LOGGER.info(
        "CREDIT created: id=%s type=%s requested=%s EUR status=%s",
        credit_id,
        credit_type,
        requested_amount,
        status,
    )
    return credit_id


def insert_one_of_each(connection: PgConnection) -> None:
    insert_transaction(connection)
    insert_payment(connection)
    insert_credit(connection)


def choose_event_type() -> str:
    return random.choices(
        ["transaction", "payment", "credit"],
        weights=[60, 30, 10],
        k=1,
    )[0]


def insert_random_event(connection: PgConnection) -> None:
    event_type = choose_event_type()
    if event_type == "transaction":
        insert_transaction(connection)
    elif event_type == "payment":
        insert_payment(connection)
    else:
        insert_credit(connection)


def handle_stop_signal(signum: int, _frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    LOGGER.info("Stop signal received: %s", signum)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate fictional Core Banking records in PostgreSQL."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Insert one transaction, one payment, and one credit, then exit.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between generated events in continuous mode (default: 5).",
    )
    parser.add_argument(
        "--burst",
        type=int,
        default=1,
        help="Number of events generated per interval (default: 1).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    if args.interval <= 0:
        raise ValueError("--interval must be greater than zero")
    if args.burst <= 0:
        raise ValueError("--burst must be greater than zero")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    signal.signal(signal.SIGINT, handle_stop_signal)
    signal.signal(signal.SIGTERM, handle_stop_signal)

    random.seed(uuid.uuid4().int)
    config = DatabaseConfig.from_environment()
    connection = connect(config)

    try:
        if args.once:
            insert_one_of_each(connection)
            return 0

        LOGGER.info(
            "Continuous simulation started: interval=%ss burst=%s",
            args.interval,
            args.burst,
        )

        while not STOP_REQUESTED:
            for _ in range(args.burst):
                if STOP_REQUESTED:
                    break
                try:
                    insert_random_event(connection)
                except Exception:
                    connection.rollback()
                    LOGGER.exception("Failed to create a synthetic banking record")

            if not STOP_REQUESTED:
                time.sleep(args.interval)

        return 0

    finally:
        connection.close()
        LOGGER.info("Simulator stopped")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        LOGGER.exception("Simulator terminated with an error")
        sys.exit(1)
