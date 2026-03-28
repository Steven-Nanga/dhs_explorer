-- Widen status so loader can store "completed_with_errors" and similar
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'catalog'
          AND table_name = 'import_batch'
          AND column_name = 'status'
          AND character_maximum_length = 20
    ) THEN
        ALTER TABLE catalog.import_batch
            ALTER COLUMN status TYPE VARCHAR(64);
    END IF;
END $$;
