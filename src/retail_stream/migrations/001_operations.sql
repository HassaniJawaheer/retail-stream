CREATE TABLE products (
    product_id UUID NOT NULL UNIQUE,
    sku TEXT PRIMARY KEY CHECK (sku ~ '^[A-Z0-9][A-Z0-9_-]{0,63}$'),
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    manufacturer TEXT NOT NULL CHECK (length(trim(manufacturer)) > 0),
    model TEXT NOT NULL CHECK (length(trim(model)) > 0),
    category TEXT NOT NULL CHECK (length(trim(category)) > 0),
    description TEXT NOT NULL CHECK (length(trim(description)) > 0),
    price_cents BIGINT NOT NULL CHECK (price_cents > 0 AND price_cents <= 100000000),
    currency TEXT NOT NULL CHECK (currency = 'EUR'),
    status TEXT NOT NULL CHECK (status IN ('active', 'inactive', 'discontinued')),
    source TEXT,
    source_ref TEXT,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((source IS NULL) = (source_ref IS NULL)),
    UNIQUE (source, source_ref)
);
CREATE TABLE warehouses (warehouse_id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE inventory (
    warehouse_id TEXT REFERENCES warehouses,
    sku TEXT REFERENCES products,
    physical BIGINT NOT NULL CHECK (physical >= 0),
    reserved BIGINT NOT NULL DEFAULT 0 CHECK (reserved >= 0 AND reserved <= physical),
    safety_stock BIGINT NOT NULL DEFAULT 0 CHECK (safety_stock >= 0),
    reorder_point BIGINT NOT NULL DEFAULT 5 CHECK (reorder_point >= 0),
    target_stock BIGINT NOT NULL DEFAULT 30 CHECK (target_stock > reorder_point),
    version BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (warehouse_id, sku)
);
CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    warehouse_id TEXT REFERENCES warehouses,
    status TEXT NOT NULL CHECK (status IN ('created','reserved','rejected','cancelled','expired','shipped')),
    reason TEXT,
    total_amount_cents BIGINT NOT NULL CHECK (total_amount_cents >= 0),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    action_at TIMESTAMPTZ,
    planned_action TEXT CHECK (planned_action IN ('ship','cancel'))
);
CREATE TABLE order_items (
    order_id UUID REFERENCES orders,
    sku TEXT REFERENCES products,
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents > 0),
    product_version BIGINT NOT NULL,
    PRIMARY KEY (order_id, sku)
);
CREATE TABLE reservations (
    reservation_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE REFERENCES orders,
    status TEXT NOT NULL CHECK (status IN ('active','released','consumed','expired')),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE reservation_items (
    reservation_id UUID REFERENCES reservations,
    warehouse_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    FOREIGN KEY (warehouse_id, sku) REFERENCES inventory,
    PRIMARY KEY (reservation_id, sku)
);
CREATE TABLE replenishments (
    replenishment_id UUID PRIMARY KEY,
    request_id TEXT UNIQUE NOT NULL,
    warehouse_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    status TEXT NOT NULL CHECK (status IN ('requested','received')),
    due_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (warehouse_id, sku) REFERENCES inventory
);
CREATE UNIQUE INDEX one_open_replenishment ON replenishments (warehouse_id, sku)
WHERE status = 'requested';
CREATE TABLE stock_movements (
    movement_id UUID PRIMARY KEY,
    warehouse_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    reason TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    physical_delta BIGINT NOT NULL,
    reserved_delta BIGINT NOT NULL,
    physical_after BIGINT NOT NULL,
    reserved_after BIGINT NOT NULL,
    stock_version BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (warehouse_id, sku) REFERENCES inventory,
    UNIQUE (warehouse_id, sku, stock_version)
);
CREATE TABLE outbox (
    sequence BIGSERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX pending_outbox ON outbox (sequence) WHERE published_at IS NULL;
CREATE TABLE analytics_events (
    event_id UUID PRIMARY KEY,
    event JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE inventory_projection (
    warehouse_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    physical BIGINT NOT NULL,
    reserved BIGINT NOT NULL,
    available BIGINT NOT NULL,
    stock_version BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (warehouse_id, sku)
);
CREATE TABLE order_projection (
    order_id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    reason TEXT,
    entity_version BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL
);
CREATE TABLE business_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    amount_cents NUMERIC(38,0) NOT NULL,
    PRIMARY KEY (window_start, event_type, reason)
);
CREATE TABLE movement_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    sku TEXT NOT NULL,
    reason TEXT NOT NULL,
    movement_count BIGINT NOT NULL,
    physical_delta BIGINT NOT NULL,
    reserved_delta BIGINT NOT NULL,
    PRIMARY KEY (window_start, sku, reason)
);
CREATE TABLE replenishment_projection (
    replenishment_id UUID PRIMARY KEY,
    sku TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    requested_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ
);
