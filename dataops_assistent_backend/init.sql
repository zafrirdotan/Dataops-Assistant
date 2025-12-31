-- Database initialization script for DataOps Assistant
-- This file will be executed when PostgreSQL container starts

-- Create schemas if needed
CREATE SCHEMA IF NOT EXISTS dataops_assistent;
CREATE SCHEMA IF NOT EXISTS dw;

-- Create tables for storing pipeline metadata
CREATE TABLE IF NOT EXISTS dataops_assistent.pipelines (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    created_by VARCHAR,
    description VARCHAR,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    status VARCHAR,
    run_list JSON,
    spec JSON,
    image_id VARCHAR
);

-- Create tables for storing pipeline executions
CREATE TABLE IF NOT EXISTS dataops_assistent.pipeline_executions (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES dataops_assistent.pipelines(id),
    status VARCHAR(50) DEFAULT 'pending',
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    logs TEXT,
    input_data JSONB,
    output_data JSONB
);

-- Create transactions table for sample data
CREATE TABLE IF NOT EXISTS public.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    user_id INTEGER,
    account_id INTEGER,
    transaction_date DATE,
    transaction_time TIME,
    amount DECIMAL(10,2),
    currency VARCHAR(3),
    merchant VARCHAR(100),
    category VARCHAR(50),
    transaction_type VARCHAR(20),
    status VARCHAR(20),
    location VARCHAR(100),
    device VARCHAR(20),
    balance_after DECIMAL(12,2),
    notes TEXT
);