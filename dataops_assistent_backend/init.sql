-- Database initialization script for DataOps Assistant
-- This file will be executed when PostgreSQL container starts

-- Create schemas if needed
CREATE SCHEMA IF NOT EXISTS dataops_assistent;
CREATE SCHEMA IF NOT EXISTS dw;

-- Create tables for storing pipeline metadata
CREATE TABLE IF NOT EXISTS dataops_assistent.pipelines (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    description VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ,
    status VARCHAR,
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

-- Create users table for authentication
CREATE TABLE IF NOT EXISTS public.users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    is_superuser BOOLEAN DEFAULT false NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Create indexes for users table
CREATE INDEX IF NOT EXISTS idx_users_username ON public.users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON public.users(email);

-- Create message content type enum
DO $$ BEGIN
    CREATE TYPE dataops_assistent.message_content_type AS ENUM ('text', 'code', 'steps', 'error');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create chats table for managing conversations
CREATE TABLE IF NOT EXISTS dataops_assistent.chats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pipeline
        FOREIGN KEY (pipeline_id)
        REFERENCES dataops_assistent.pipelines(pipeline_id)
        ON DELETE SET NULL
);

-- Create chat_messages table for storing conversation history
CREATE TABLE IF NOT EXISTS dataops_assistent.chat_messages (
    id SERIAL PRIMARY KEY,
    chat_id UUID NOT NULL,
    role VARCHAR NOT NULL,
    content TEXT NOT NULL,
    extra_data JSONB, -- Contains: { type: 'code' | 'steps'?, steps: [...], Code: '...' }
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT fk_chat
        FOREIGN KEY (chat_id)
        REFERENCES dataops_assistent.chats(id)
        ON DELETE CASCADE
);

-- Create indexes for chats table
CREATE INDEX IF NOT EXISTS idx_chats_pipeline_id ON dataops_assistent.chats(pipeline_id);

-- Create indexes for chat_messages table
CREATE INDEX IF NOT EXISTS idx_chat_messages_chat_id ON dataops_assistent.chat_messages(chat_id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON dataops_assistent.chat_messages(created_at);

-- Create steps table for storing pipeline execution steps
CREATE TABLE IF NOT EXISTS dataops_assistent.steps (
    id SERIAL PRIMARY KEY,
    chat_id UUID NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    step_type VARCHAR(100),
    status VARCHAR(50) DEFAULT 'pending',
    code TEXT,
    output TEXT,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    extra_data JSONB,
    CONSTRAINT fk_step_chat
        FOREIGN KEY (chat_id)
        REFERENCES dataops_assistent.chats(id)
        ON DELETE CASCADE
);

-- Create indexes for steps table
CREATE INDEX IF NOT EXISTS idx_steps_chat_id ON dataops_assistent.steps(chat_id);
CREATE INDEX IF NOT EXISTS idx_steps_status ON dataops_assistent.steps(status);
