-- Database initialization script for DataOps Assistant
-- This file will be executed when PostgreSQL container starts

-- Create schemas if needed
CREATE SCHEMA IF NOT EXISTS dataops;
CREATE SCHEMA IF NOT EXISTS dataops_assistent;

-- Create tables for storing pipeline metadata
CREATE TABLE IF NOT EXISTS pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    spec JSONB,
    code TEXT,
    status VARCHAR(50) DEFAULT 'created',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create tables for storing pipeline executions
CREATE TABLE IF NOT EXISTS pipeline_executions (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),
    status VARCHAR(50) DEFAULT 'pending',
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    logs TEXT,
    input_data JSONB,
    output_data JSONB
);

-- Create tables for storing chat history
CREATE TABLE IF NOT EXISTS chat_history (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255),
    user_message TEXT NOT NULL,
    assistant_response TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name);
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_pipeline_id ON pipeline_executions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status ON pipeline_executions(status);
CREATE INDEX IF NOT EXISTS idx_chat_history_session_id ON chat_history(session_id);
CREATE INDEX IF NOT EXISTS idx_chat_history_created_at ON chat_history(created_at);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_pipelines_updated_at 
    BEFORE UPDATE ON pipelines 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

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

-- Insert some sample data (optional)
INSERT INTO pipelines (name, description, spec, code, status) 
VALUES 
    ('sample_pipeline', 'A sample data pipeline', '{"type": "etl", "source": "csv"}', '# Sample pipeline code', 'active')
ON CONFLICT (name) DO NOTHING;
