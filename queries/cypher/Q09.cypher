MATCH (content)-[:HAS_CREATOR]->(person:Person)-[:IS_LOCATED_IN]->(location:Place)
WHERE (content:Post OR content:Comment)
  AND content.creationDate >= 1325376000000
RETURN
  person.id AS personId,
  content.id AS contentId,
  location.id AS locationId
ORDER BY content.creationDate DESC
LIMIT 10;