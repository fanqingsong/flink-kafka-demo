CREATE TABLE experiments (
    id UUID PRIMARY KEY,
    experiment_key VARCHAR(120) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('DRAFT', 'RUNNING', 'PAUSED')),
    salt VARCHAR(120) NOT NULL,
    traffic_percent INTEGER NOT NULL CHECK (traffic_percent BETWEEN 0 AND 100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE experiment_variants (
    id UUID PRIMARY KEY,
    experiment_id UUID NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    variant_key VARCHAR(120) NOT NULL,
    name VARCHAR(200) NOT NULL,
    weight INTEGER NOT NULL CHECK (weight > 0),
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (experiment_id, variant_key)
);

INSERT INTO experiments (
    id, experiment_key, name, status, salt, traffic_percent
) VALUES (
    '11111111-1111-1111-1111-111111111111',
    'checkout-button',
    '结算按钮文案',
    'RUNNING',
    'checkout-button-v1',
    100
);

INSERT INTO experiment_variants (
    id, experiment_id, variant_key, name, weight, payload_json
) VALUES
(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    '11111111-1111-1111-1111-111111111111',
    'control',
    '立即购买',
    50,
    '{"button_text":"立即购买","button_color":"#0d6b58"}'
),
(
    'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    '11111111-1111-1111-1111-111111111111',
    'treatment',
    '马上抢购',
    50,
    '{"button_text":"马上抢购","button_color":"#b45309"}'
);
