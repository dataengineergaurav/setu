-- ──────────────────────────────────────────────────────────────
-- Test data seeder
-- ──────────────────────────────────────────────────────────────
-- Run this after the stack is up to generate WAL events:
--
--   docker compose -f demo/docker-compose.yml exec postgres \
--     psql -U postgres -d activation_demo -f /docker-entrypoint-initdb.d/seed.sql
--
-- Or pipe it in:
--   docker compose -f demo/docker-compose.yml exec -T postgres \
--     psql -U postgres -d activation_demo < demo/seed.sql

BEGIN;

-- ── 1. User upgrades plan (triggers Update on users) ──
UPDATE users SET plan = 'premium', status = 'premium'
WHERE id = 1 AND plan = 'free';

-- ── 2. New order placed (triggers Insert on orders) ──
INSERT INTO orders (user_id, product, amount_cents, status)
VALUES (2, 'Enterprise Bundle', 49999, 'pending');

-- ── 3. Inventory drops below threshold (triggers Update on inventory) ──
UPDATE inventory SET quantity = 5 WHERE sku = 'WIDGET-PRO';

-- ── 4. Order status changes (triggers Update on orders) ──
UPDATE orders SET status = 'shipped' WHERE id = 2;

-- ── 5. User deleted (triggers Delete on users) ──
DELETE FROM orders WHERE id = 3;
DELETE FROM users WHERE id = 3;

COMMIT;

-- Show what just happened
\timing off
\echo ''
\echo '=== Seed data applied! Check http://localhost:8080 for events ==='
\echo ''
