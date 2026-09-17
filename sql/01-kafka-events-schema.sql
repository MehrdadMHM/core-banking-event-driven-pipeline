--
-- PostgreSQL database dump
--

-- Dumped from database version 15.13 (Debian 15.13-1.pgdg120+1)
-- Dumped by pg_dump version 15.13 (Debian 15.13-1.pgdg120+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: control; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA control;


--
-- Name: core; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA core;


--
-- Name: stage; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA stage;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: pipeline_watermark; Type: TABLE; Schema: control; Owner: -
--

CREATE TABLE control.pipeline_watermark (
    source_schema character varying(100) NOT NULL,
    source_table character varying(100) NOT NULL,
    id_column character varying(100) NOT NULL,
    last_processed_id bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: credit; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.credit (
    credit_id bigint NOT NULL,
    customer_id bigint NOT NULL,
    credit_type character varying(30) NOT NULL,
    requested_amount numeric(18,2) NOT NULL,
    approved_amount numeric(18,2),
    currency character varying(3) NOT NULL,
    interest_rate numeric(5,2),
    term_months integer,
    status character varying(20) DEFAULT 'REQUESTED'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT chk_approved_amount CHECK (((approved_amount IS NULL) OR (approved_amount >= (0)::numeric))),
    CONSTRAINT chk_credit_status CHECK (((status)::text = ANY ((ARRAY['REQUESTED'::character varying, 'UNDER_REVIEW'::character varying, 'APPROVED'::character varying, 'REJECTED'::character varying, 'ACTIVE'::character varying, 'CLOSED'::character varying])::text[]))),
    CONSTRAINT chk_credit_type CHECK (((credit_type)::text = ANY ((ARRAY['PERSONAL'::character varying, 'MORTGAGE'::character varying, 'CAR'::character varying, 'OVERDRAFT'::character varying])::text[]))),
    CONSTRAINT chk_interest_rate CHECK (((interest_rate IS NULL) OR (interest_rate >= (0)::numeric))),
    CONSTRAINT chk_term_months CHECK (((term_months IS NULL) OR (term_months > 0))),
    CONSTRAINT credit_requested_amount_check CHECK ((requested_amount > (0)::numeric))
);


--
-- Name: credit_credit_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE core.credit_credit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: credit_credit_id_seq; Type: SEQUENCE OWNED BY; Schema: core; Owner: -
--

ALTER SEQUENCE core.credit_credit_id_seq OWNED BY core.credit.credit_id;


--
-- Name: payments; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.payments (
    payment_id bigint NOT NULL,
    debtor_account bigint NOT NULL,
    creditor_account bigint NOT NULL,
    payment_method character varying(30) NOT NULL,
    amount numeric(18,2) NOT NULL,
    currency character varying(3) NOT NULL,
    status character varying(20) DEFAULT 'CREATED'::character varying NOT NULL,
    execution_date date DEFAULT CURRENT_DATE NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT chk_payment_method CHECK (((payment_method)::text = ANY ((ARRAY['SEPA'::character varying, 'INSTANT'::character varying, 'CARD'::character varying, 'DIRECT_DEBIT'::character varying])::text[]))),
    CONSTRAINT chk_payment_status CHECK (((status)::text = ANY ((ARRAY['CREATED'::character varying, 'PROCESSING'::character varying, 'COMPLETED'::character varying, 'FAILED'::character varying, 'CANCELLED'::character varying])::text[]))),
    CONSTRAINT payments_amount_check CHECK ((amount > (0)::numeric))
);


--
-- Name: payments_payment_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE core.payments_payment_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: payments_payment_id_seq; Type: SEQUENCE OWNED BY; Schema: core; Owner: -
--

ALTER SEQUENCE core.payments_payment_id_seq OWNED BY core.payments.payment_id;


--
-- Name: transactions; Type: TABLE; Schema: core; Owner: -
--

CREATE TABLE core.transactions (
    transaction_id bigint NOT NULL,
    account_id bigint NOT NULL,
    transaction_type character varying(30) NOT NULL,
    amount numeric(18,2) NOT NULL,
    currency character varying(3) NOT NULL,
    status character varying(20) DEFAULT 'PENDING'::character varying NOT NULL,
    booking_date date DEFAULT CURRENT_DATE NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT chk_transaction_status CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'COMPLETED'::character varying, 'REJECTED'::character varying, 'CANCELLED'::character varying])::text[]))),
    CONSTRAINT chk_transaction_type CHECK (((transaction_type)::text = ANY ((ARRAY['DEPOSIT'::character varying, 'WITHDRAWAL'::character varying, 'TRANSFER'::character varying, 'FEE'::character varying])::text[]))),
    CONSTRAINT transactions_amount_check CHECK ((amount > (0)::numeric))
);


--
-- Name: transactions_transaction_id_seq; Type: SEQUENCE; Schema: core; Owner: -
--

CREATE SEQUENCE core.transactions_transaction_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transactions_transaction_id_seq; Type: SEQUENCE OWNED BY; Schema: core; Owner: -
--

ALTER SEQUENCE core.transactions_transaction_id_seq OWNED BY core.transactions.transaction_id;


--
-- Name: payment_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.payment_events (
    event_id bigint NOT NULL,
    event_type character varying(100) NOT NULL,
    amount numeric(18,2),
    currency character varying(3),
    kafka_topic character varying(200),
    partition_id integer,
    kafka_offset bigint,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: raw_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_events (
    id bigint NOT NULL,
    event_id character varying(100),
    event_type character varying(100),
    payload jsonb NOT NULL,
    kafka_topic character varying(200),
    partition_id integer,
    kafka_offset bigint,
    processing_status character varying(20) DEFAULT 'NEW'::character varying,
    received_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    processed_at timestamp with time zone,
    error_message text
);


--
-- Name: raw_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.raw_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.raw_events_id_seq OWNED BY public.raw_events.id;


--
-- Name: credit; Type: TABLE; Schema: stage; Owner: -
--

CREATE TABLE stage.credit (
    credit_id bigint NOT NULL,
    customer_id bigint NOT NULL,
    credit_type character varying(30) NOT NULL,
    requested_amount numeric(18,2) NOT NULL,
    approved_amount numeric(18,2),
    currency character varying(3) NOT NULL,
    interest_rate numeric(5,2),
    term_months integer,
    status character varying(20) NOT NULL,
    event_timestamp timestamp with time zone NOT NULL,
    kafka_topic character varying(200) NOT NULL,
    partition_id integer NOT NULL,
    kafka_offset bigint NOT NULL,
    loaded_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: payments; Type: TABLE; Schema: stage; Owner: -
--

CREATE TABLE stage.payments (
    payment_id bigint NOT NULL,
    debtor_account bigint NOT NULL,
    creditor_account bigint NOT NULL,
    payment_method character varying(30) NOT NULL,
    amount numeric(18,2) NOT NULL,
    currency character varying(3) NOT NULL,
    status character varying(20) NOT NULL,
    execution_date date NOT NULL,
    event_timestamp timestamp with time zone NOT NULL,
    kafka_topic character varying(200) NOT NULL,
    partition_id integer NOT NULL,
    kafka_offset bigint NOT NULL,
    loaded_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: transactions; Type: TABLE; Schema: stage; Owner: -
--

CREATE TABLE stage.transactions (
    transaction_id bigint NOT NULL,
    account_id bigint NOT NULL,
    transaction_type character varying(30) NOT NULL,
    amount numeric(18,2) NOT NULL,
    currency character varying(3) NOT NULL,
    status character varying(20) NOT NULL,
    booking_date date NOT NULL,
    event_timestamp timestamp with time zone NOT NULL,
    kafka_topic character varying(200) NOT NULL,
    partition_id integer NOT NULL,
    kafka_offset bigint NOT NULL,
    loaded_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: credit credit_id; Type: DEFAULT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.credit ALTER COLUMN credit_id SET DEFAULT nextval('core.credit_credit_id_seq'::regclass);


--
-- Name: payments payment_id; Type: DEFAULT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.payments ALTER COLUMN payment_id SET DEFAULT nextval('core.payments_payment_id_seq'::regclass);


--
-- Name: transactions transaction_id; Type: DEFAULT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.transactions ALTER COLUMN transaction_id SET DEFAULT nextval('core.transactions_transaction_id_seq'::regclass);


--
-- Name: raw_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_events ALTER COLUMN id SET DEFAULT nextval('public.raw_events_id_seq'::regclass);


--
-- Name: pipeline_watermark pk_pipeline_watermark; Type: CONSTRAINT; Schema: control; Owner: -
--

ALTER TABLE ONLY control.pipeline_watermark
    ADD CONSTRAINT pk_pipeline_watermark PRIMARY KEY (source_schema, source_table);


--
-- Name: credit credit_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.credit
    ADD CONSTRAINT credit_pkey PRIMARY KEY (credit_id);


--
-- Name: payments payments_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.payments
    ADD CONSTRAINT payments_pkey PRIMARY KEY (payment_id);


--
-- Name: transactions transactions_pkey; Type: CONSTRAINT; Schema: core; Owner: -
--

ALTER TABLE ONLY core.transactions
    ADD CONSTRAINT transactions_pkey PRIMARY KEY (transaction_id);


--
-- Name: payment_events payment_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_events
    ADD CONSTRAINT payment_events_pkey PRIMARY KEY (event_id);


--
-- Name: raw_events raw_events_kafka_topic_partition_id_kafka_offset_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_events
    ADD CONSTRAINT raw_events_kafka_topic_partition_id_kafka_offset_key UNIQUE (kafka_topic, partition_id, kafka_offset);


--
-- Name: raw_events raw_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_events
    ADD CONSTRAINT raw_events_pkey PRIMARY KEY (id);


--
-- Name: credit credit_pkey; Type: CONSTRAINT; Schema: stage; Owner: -
--

ALTER TABLE ONLY stage.credit
    ADD CONSTRAINT credit_pkey PRIMARY KEY (credit_id);


--
-- Name: payments payments_pkey; Type: CONSTRAINT; Schema: stage; Owner: -
--

ALTER TABLE ONLY stage.payments
    ADD CONSTRAINT payments_pkey PRIMARY KEY (payment_id);


--
-- Name: transactions transactions_pkey; Type: CONSTRAINT; Schema: stage; Owner: -
--

ALTER TABLE ONLY stage.transactions
    ADD CONSTRAINT transactions_pkey PRIMARY KEY (transaction_id);


--
-- Name: credit uq_stage_credit_kafka_position; Type: CONSTRAINT; Schema: stage; Owner: -
--

ALTER TABLE ONLY stage.credit
    ADD CONSTRAINT uq_stage_credit_kafka_position UNIQUE (kafka_topic, partition_id, kafka_offset);


--
-- Name: payments uq_stage_payments_kafka_position; Type: CONSTRAINT; Schema: stage; Owner: -
--

ALTER TABLE ONLY stage.payments
    ADD CONSTRAINT uq_stage_payments_kafka_position UNIQUE (kafka_topic, partition_id, kafka_offset);


--
-- Name: transactions uq_stage_transactions_kafka_position; Type: CONSTRAINT; Schema: stage; Owner: -
--

ALTER TABLE ONLY stage.transactions
    ADD CONSTRAINT uq_stage_transactions_kafka_position UNIQUE (kafka_topic, partition_id, kafka_offset);


--
-- Name: idx_credit_created_at; Type: INDEX; Schema: core; Owner: -
--

CREATE INDEX idx_credit_created_at ON core.credit USING btree (created_at);


--
-- Name: idx_payments_created_at; Type: INDEX; Schema: core; Owner: -
--

CREATE INDEX idx_payments_created_at ON core.payments USING btree (created_at);


--
-- Name: idx_transactions_created_at; Type: INDEX; Schema: core; Owner: -
--

CREATE INDEX idx_transactions_created_at ON core.transactions USING btree (created_at);


--
-- Name: idx_stage_credit_customer; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_credit_customer ON stage.credit USING btree (customer_id);


--
-- Name: idx_stage_credit_status; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_credit_status ON stage.credit USING btree (status);


--
-- Name: idx_stage_credit_type; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_credit_type ON stage.credit USING btree (credit_type);


--
-- Name: idx_stage_payments_creditor; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_payments_creditor ON stage.payments USING btree (creditor_account);


--
-- Name: idx_stage_payments_debtor; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_payments_debtor ON stage.payments USING btree (debtor_account);


--
-- Name: idx_stage_payments_execution_date; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_payments_execution_date ON stage.payments USING btree (execution_date);


--
-- Name: idx_stage_payments_status; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_payments_status ON stage.payments USING btree (status);


--
-- Name: idx_stage_transactions_account; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_transactions_account ON stage.transactions USING btree (account_id);


--
-- Name: idx_stage_transactions_booking_date; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_transactions_booking_date ON stage.transactions USING btree (booking_date);


--
-- Name: idx_stage_transactions_status; Type: INDEX; Schema: stage; Owner: -
--

CREATE INDEX idx_stage_transactions_status ON stage.transactions USING btree (status);


--
-- PostgreSQL database dump complete
--

