MATCH (person:Person)-[:KNOWS]->(friend:Person)-[:KNOWS]->(fofPerson:Person)
WHERE fofPerson <> person
RETURN
person.id AS personId,
COUNT(DISTINCT fofPerson) AS friendsOfFriendsCount
ORDER BY friendsOfFriendsCount DESC, personId
LIMIT 10;