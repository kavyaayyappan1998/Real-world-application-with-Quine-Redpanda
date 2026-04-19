MERGE (u:User {id: $userId})
WITH u
UNWIND $followers AS followerId
MERGE (f:User {id: followerId})
MERGE (f)-[:FOLLOWS]->(u)