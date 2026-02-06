-- FlowGuard PostgreSQL Initialization Script
-- Purpose: Create foods table and load reference data

-- Create foods table
CREATE TABLE IF NOT EXISTS foods (
    food_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    description TEXT,
    image_url TEXT NOT NULL,
    is_available BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on category for faster filtering
CREATE INDEX IF NOT EXISTS idx_foods_category ON foods(category);
CREATE INDEX IF NOT EXISTS idx_foods_available ON foods(is_available);

-- Note: Data will be loaded via Python script after container starts
-- This keeps JSON parsing logic in application layer
