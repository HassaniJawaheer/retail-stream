CREATE TABLE IF NOT EXISTS order_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    warehouse_id TEXT NOT NULL,
    order_count BIGINT NOT NULL CHECK (order_count > 0),
    total_amount_cents NUMERIC(38, 0) NOT NULL CHECK (total_amount_cents >= 0),
    item_count NUMERIC(38, 0) NOT NULL CHECK (item_count > 0),
    average_order_amount_cents NUMERIC(38, 2) NOT NULL,
    PRIMARY KEY (window_start, window_end, warehouse_id),
    CHECK (window_end > window_start)
);
