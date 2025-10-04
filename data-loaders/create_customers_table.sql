-- Create customers table for testing
CREATE TABLE IF NOT EXISTS public.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    created_date DATE DEFAULT CURRENT_DATE,
    status VARCHAR(20) DEFAULT 'active'
);

-- Insert sample data
INSERT INTO public.customers (first_name, last_name, email, phone, city, country) VALUES
('John', 'Doe', 'john.doe@email.com', '+1234567890', 'New York', 'USA'),
('Jane', 'Smith', 'jane.smith@email.com', '+1987654321', 'London', 'UK'),
('Carlos', 'Rodriguez', 'carlos.r@email.com', '+34612345678', 'Madrid', 'Spain'),
('Anna', 'Mueller', 'anna.m@email.com', '+49123456789', 'Berlin', 'Germany'),
('Hiroshi', 'Tanaka', 'h.tanaka@email.com', '+81987654321', 'Tokyo', 'Japan')
ON CONFLICT (email) DO NOTHING;
