# Cypher Queries

Create Users:

    WITH ["Jennifer","Michelle","Tanya","Julie","Christie","Sophie","Amanda","Khloe","Sarah","Kaylee"] AS names 
    FOREACH (r IN range(0,100000) | CREATE (:User {username:names[r % size(names)]+r}));

Create Relationships:

    MATCH (u1:User),(u2:User)
    WITH u1,u2
    LIMIT 5000000
    WHERE rand() < 0.1
    CREATE (u1)-[:FRIENDS]->(u2);

Create Index:

    CREATE INDEX ON :User(username);
    
    