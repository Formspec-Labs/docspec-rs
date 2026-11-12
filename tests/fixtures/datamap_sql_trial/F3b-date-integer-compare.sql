-- What happened to this rule, and when?
SELECT event_date, event, title, fr_document_number, regulations_document_id
FROM rulemaking_timeline
WHERE rulemaking_id = $1 AND event_date >= 20200101
ORDER BY event_date, event, source_key
