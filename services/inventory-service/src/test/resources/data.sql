-- Test inventory data
INSERT INTO inventory_items (product_id, available_quantity, reserved_quantity, version)
VALUES ('PROD-001', 100, 0, 0)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO inventory_items (product_id, available_quantity, reserved_quantity, version)
VALUES ('PROD-002', 50, 0, 0)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO inventory_items (product_id, available_quantity, reserved_quantity, version)
VALUES ('PROD-003', 0, 0, 0)
ON CONFLICT (product_id) DO NOTHING;
