MATCH (post:Post)-[:HAS_CREATOR]->(person:Person)
MATCH (person)-[:IS_LOCATED_IN]->(location:Place)
RETURN person.id AS personId,
       post.id AS postId,
       location.id AS locationId
ORDER BY personId, postId, locationId
LIMIT 10;