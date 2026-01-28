-- Migration: Add Mail Type Priorities and Update Conditional Email Delay
-- Run this migration manually or via Prisma migrate

-- 1. Add mailTypePriorities column to settings table
ALTER TABLE settings 
ADD COLUMN IF NOT EXISTS mail_type_priorities JSONB 
DEFAULT '{"initial": 1, "followup": 2, "manual": 3, "conditional": 4}';

-- 2. Rename delay_minutes to delay_hours in conditional_emails
-- First check if the column exists with old name
DO $$ 
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'conditional_emails' AND column_name = 'delay_minutes'
    ) THEN
        -- Rename the column
        ALTER TABLE conditional_emails RENAME COLUMN delay_minutes TO delay_hours;
        
        -- Note: Existing values in minutes will be treated as hours after this migration
        -- Consider updating values if necessary (e.g., divide by 60 or set to 0)
        UPDATE conditional_emails SET delay_hours = 0 WHERE delay_hours > 0;
    END IF;
END $$;

-- 3. Create index on settings for faster lookups (optional optimization)
CREATE INDEX IF NOT EXISTS idx_settings_updated_at ON settings(updated_at);
