MATCH (person:Person)
RETURN
  person.id AS id
ORDER BY person.creationDate DESC
LIMIT 10;