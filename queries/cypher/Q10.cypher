MATCH (person:Person)-[:IS_LOCATED_IN]->(location:Place)
MATCH (person)-[:KNOWS]->(friend:Person)-[:IS_LOCATED_IN]->(location)
RETURN person.id AS personId,
COUNT(DISTINCT friend) AS localFriendsCount
ORDER BY localFriendsCount DESC, personId
LIMIT 10;