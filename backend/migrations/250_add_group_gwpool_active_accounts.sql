-- Existing groups and newly created groups both start at 1. The former global
-- setting deliberately does not seed this field.
ALTER TABLE groups
    ADD COLUMN IF NOT EXISTS openai_gwpool_active_accounts INTEGER NOT NULL DEFAULT 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'groups'::regclass
          AND conname = 'groups_openai_gwpool_active_accounts_range'
    ) THEN
        ALTER TABLE groups ADD CONSTRAINT groups_openai_gwpool_active_accounts_range
            CHECK (openai_gwpool_active_accounts BETWEEN 1 AND 64);
    END IF;
END $$;
