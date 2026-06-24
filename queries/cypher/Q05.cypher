MATCH (person:Person)
OPTIONAL MATCH (person)-[:KNOWS]->(friend:Person)
RETURN
  person.id AS personId,
  COUNT(DISTINCT friend) AS friendCount
ORDER BY friendCount DESC, personId
LIMIT 10;