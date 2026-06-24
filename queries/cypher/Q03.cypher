MATCH (person:Person)
WHERE NOT EXISTS {
  (:Post)-[:HAS_CREATOR]->(person)
}
RETURN person.id AS personId
ORDER BY personId
LIMIT 10;