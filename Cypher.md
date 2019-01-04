# Cypher Queries

Create Users:

    WITH ["Jennifer","Michelle","Tanya","Julie","Christie","Sophie","Amanda","Khloe","Sarah","Kaylee"] AS names 
    FOREACH (r IN range(0,100000) | CREATE (:User {username:names[r % size(names)]+r}));

Create Relationships:

    MATCH (u1:User),(u2:User)
    WITH u1,u2
    LIMIT 5000000
    WHERE rand() < 0.1
    CREATE (u1)-[:FRIENDS {weight: rand()}]->(u2);

Create Index:

    CREATE INDEX ON :User(username);


Get the network side for Khloe17 4 hops out:

    MATCH (u:User{username:'Khloe17'})-[*1..4]->(c) 
    RETURN count(DISTINCT c)

Takes about a second. Now take the arrow sign off:

    MATCH (u:User{username:'Khloe17'})-[*1..4]-(c) 
    RETURN count(DISTINCT c)

OMG, so long...
    
Get 10k usernames:

    MATCH (n:User) 
    RETURN n.username 
    LIMIT 10000    
    
    