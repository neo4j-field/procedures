# Cypher Queries

Create Users:

    WITH ["Jennifer","Michelle","Tanya","Julie","Christie","Sophie","Amanda","Khloe","Sarah","Kaylee"] AS names 
    FOREACH (r IN range(1,100000) | CREATE (:User {username:names[r % size(names)]+r}));

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
    
Reach Query:
    
    MATCH p=(u:User)-[*1..2]-(u2:User) 
    WHERE u.username = 'Khloe17' AND
          u2.username = 'Michelle21'      
    RETURN p, REDUCE(weight = 0.0, r in relationships(p) | weight + r.weight) / length(p) AS weight
    ORDER BY weight DESC
    LIMIT 100
    
Good Friends Query:
        
    MATCH p=(u:User)-[*1..4]-(u2:User) 
    WHERE u.username = 'Khloe17' AND u2.username = 'Michelle21' AND
          ALL (r IN relationships(p) WHERE r.weight >= 0.80)
    RETURN p, REDUCE(weight = 0.0, r in relationships(p) | weight + r.weight) / length(p) AS weight
    ORDER BY length(p), weight DESC
    LIMIT 100

Report Queries:

    MATCH (u:User)-[:FRIENDS]-(u2:User)-[:FRIENDS]-(u3:User)
    RETURN u.username, u2.username, u3.username
    LIMIT 100

    EXPLAIN MATCH (u:User)-[:FRIENDS]-(u2:User)-[:FRIENDS]-(u3:User)
    RETURN u.username, u2.username, u3.username
    ORDER BY u.username, u2.username, u3.username
    LIMIT 100

    EXPLAIN MATCH (u:User)-[:FRIENDS]-(u2:User)-[:FRIENDS]-(u3:User)
    RETURN u.username, u3.username, COUNT(u2)
    LIMIT 100

Analytic Query:

    EXPLAIN MATCH (u:User)-[:FRIENDS]-(u2:User)-[:FRIENDS]-(u3:User)
    WITH u, u3, COUNT(u2) AS counts
    RETURN counts, COUNT(*)
    ORDER BY counts
