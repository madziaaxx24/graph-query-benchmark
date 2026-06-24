MATCH (post:Post)-[:HAS_CREATOR]->(person:Person)
RETURN person.id AS personId,
       COUNT(post) AS postCount
ORDER BY postCount DESC
LIMIT 10;