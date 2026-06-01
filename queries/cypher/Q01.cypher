MATCH (person:Person {id: 933})
RETURN
  person.firstName AS firstName,
  person.lastName AS lastName,
  person.gender AS gender,
  person.birthday AS birthday;