-- Create bank_transactions table based on CSV structure
CREATE TABLE IF NOT EXISTS public.bank_transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    user_id INTEGER,
    account_id INTEGER,
    transaction_date DATE,
    transaction_time TIME,
    amount DECIMAL(12,2),
    currency VARCHAR(5),
    merchant VARCHAR(100),
    category VARCHAR(50),
    transaction_type VARCHAR(20),
    status VARCHAR(20),
    location VARCHAR(100),
    device VARCHAR(20),
    balance_after DECIMAL(12,2),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_bank_transactions_user_id ON public.bank_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_bank_transactions_date ON public.bank_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_bank_transactions_status ON public.bank_transactions(status);
CREATE INDEX IF NOT EXISTS idx_bank_transactions_category ON public.bank_transactions(category);
CREATE INDEX IF NOT EXISTS idx_bank_transactions_merchant ON public.bank_transactions(merchant);

-- Sample data from your CSV (first 10 rows)
INSERT INTO public.bank_transactions (
    transaction_id, user_id, account_id, transaction_date, transaction_time, 
    amount, currency, merchant, category, transaction_type, status, 
    location, device, balance_after, notes
) VALUES
('T000000', 1760, 37020, '2024-01-28', '21:56:11', -1499.83, 'USD', 'Tesla', 'Travel', 'Debit', 'Failed', 'New York', 'Mobile', 8861.21, ''),
('T000001', 3195, 64951, '2024-05-31', '05:56:13', -813.71, 'GBP', 'Apple', 'Entertainment', 'Debit', 'Completed', 'Tokyo', 'Web', 18646.33, 'Bonus'),
('T000002', 5502, 58067, '2023-10-28', '05:31:19', -1679.69, 'EUR', 'Google', 'Entertainment', 'Debit', 'Pending', 'Berlin', 'Web', 19849.21, 'Refund'),
('T000003', 2358, 93516, '2022-09-29', '05:44:16', 562.32, 'EUR', 'Walmart', 'Entertainment', 'Credit', 'Failed', 'London', 'Web', 17361.33, 'Rent payment'),
('T000004', 8491, 78251, '2021-11-01', '05:13:40', -2147.10, 'USD', 'Tesla', 'Travel', 'Debit', 'Completed', 'Berlin', 'ATM', 19955.98, 'Monthly subscription'),
('T000005', 1014, 54886, '2020-10-19', '03:26:00', 2588.03, 'EUR', 'Google', 'Groceries', 'Debit', 'Completed', 'New York', 'Mobile', 8448.46, 'Refund'),
('T000006', 6908, 78288, '2023-07-31', '18:26:03', 2718.01, 'EUR', 'Walmart', 'Groceries', 'Debit', 'Pending', 'London', 'POS', 2584.22, 'Gift'),
('T000007', 1292, 85999, '2022-12-19', '23:50:10', 2467.50, 'EUR', 'Walmart', 'Groceries', 'Credit', 'Completed', 'London', 'Web', 1321.19, 'Bonus'),
('T000008', 9315, 46039, '2023-08-07', '08:29:16', -1186.33, 'USD', 'Walmart', 'Salary', 'Debit', 'Failed', 'Berlin', 'Web', 3421.76, 'Refund'),
('T000009', 6336, 78441, '2024-06-30', '23:55:08', -742.37, 'USD', 'IKEA', 'Entertainment', 'Debit', 'Failed', 'London', 'Web', 14103.50, 'Bonus')
ON CONFLICT (transaction_id) DO NOTHING;
