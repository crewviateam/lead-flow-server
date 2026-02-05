-- Backfill category column for existing email_jobs
-- This ensures analytics correctly group jobs by Initial/Followup/Manual/Conditional

UPDATE email_jobs SET category = 
  CASE 
    WHEN LOWER(type) LIKE '%initial%' THEN 'initial'
    WHEN LOWER(type) LIKE 'manual%' OR type = 'manual' OR (metadata->>'manual')::boolean = true THEN 'manual'
    WHEN LOWER(type) LIKE 'conditional%' OR LOWER(type) LIKE 'conditional:%' THEN 'conditional'
    ELSE 'followup'
  END
WHERE category IS NULL;

-- Log count of updated rows
SELECT 
  category, 
  COUNT(*) as count 
FROM email_jobs 
GROUP BY category;
