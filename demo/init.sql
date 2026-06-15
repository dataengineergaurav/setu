-- ──────────────────────────────────────────────────────────────
-- Demo database initialization
-- ──────────────────────────────────────────────────────────────
-- Runs automatically when the PostgreSQL container starts.
-- Creates the demo tables with REPLICA IDENTITY FULL so the
-- engine receives old-row data on updates and deletes.

-- ── Users table ──
DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    plan TEXT NOT NULL DEFAULT 'free',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE users REPLICA IDENTITY FULL;

-- ── Orders table ──
DROP TABLE IF EXISTS orders CASCADE;
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    product TEXT NOT NULL,
    amount_cents INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE orders REPLICA IDENTITY FULL;

-- ── Inventory table ──
DROP TABLE IF EXISTS inventory CASCADE;
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    low_threshold INT NOT NULL DEFAULT 10
);
ALTER TABLE inventory REPLICA IDENTITY FULL;

-- ── Seed some starter data ──
INSERT INTO users (name, email, status, plan) VALUES
    ('Alice Johnson', 'alice@example.com', 'active', 'free'),
    ('Bob Smith', 'bob@example.com', 'active', 'premium'),
    ('Charlie Brown', 'charlie@example.com', 'inactive', 'free');

INSERT INTO orders (user_id, product, amount_cents, status) VALUES
    (1, 'Widget Pro', 2999, 'delivered'),
    (2, 'Gadget Max', 14999, 'processing'),
    (3, 'Starter Kit', 999, 'pending');

INSERT INTO inventory (sku, name, quantity, low_threshold) VALUES
    ('WIDGET-PRO', 'Widget Pro', 42, 10),
    ('GADGET-MAX', 'Gadget Max', 8, 10),
    ('STARTER-KIT', 'Starter Kit', 100, 20);
