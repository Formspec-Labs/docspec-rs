-- How did this member vote?
SELECT c.vote_date, c.chamber, c.question, c.result, c.bill_id, v.vote, v.vote_as_recorded
FROM member_votes AS v
JOIN roll_calls AS c ON c.number = v.roll_call_id
WHERE v.person_id = $1
ORDER BY c.vote_date DESC, c.roll_call_id
