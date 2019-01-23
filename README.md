# Procedures

Presentations
-------------

The presentations on this repository use git large file storage ( https://git-lfs.github.com/ ).
If you do not have this extension installed, just download the presentations from the github.com website directly.

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/procedures-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/procedures-1.0-SNAPSHOT.jar neo4j-enterprise-3.5.1/plugins/.
    

Restart your Neo4j Server. Your new Stored Procedures are available:

    CALL com.maxdemarzi.network.count('Khloe17', 3);
    CALL com.maxdemarzi.network.count2('Khloe17', 3);
    CALL com.maxdemarzi.network.count3('Khloe17', 3);
    CALL com.maxdemarzi.network.count4('Khloe17', 3);
    CALL com.maxdemarzi.network.count5('Khloe17', 3);    

    CALL com.maxdemarzi.reach.after('Khloe17', 'Michelle21')
    CALL com.maxdemarzi.reach.evaluator('Khloe17', 'Michelle21')
    CALL com.maxdemarzi.reach.expander('Khloe17', 'Michelle21')
    CALL com.maxdemarzi.reach.both('Khloe17', 'Michelle21')
        
    CALL com.maxdemarzi.fic('/Users/maxdemarzi/Documents/Projects/procedures/fic.csv')
    
    CALL com.maxdemarzi.fic.distribution
    
Network Count:

    MATCH (u:User{username:'Khloe17'})-[*1..4]-(c) 
    RETURN count(DISTINCT c)
    

Reach:

    MATCH p=(u:User)-[*1..4]-(u2:User) 
    WHERE u.username = 'Khloe17' AND u2.username = 'Michelle21' AND
          ALL (r IN relationships(p) WHERE r.weight >= 0.80)
    RETURN p, REDUCE(weight = 0.0, r in relationships(p) | weight + r.weight) / length(p) AS weight
    ORDER BY weight DESC
    LIMIT 100
    
Friends:

    MATCH (u:User)-[:FRIENDS]-(u2:User)-[:FRIENDS]-(u3:User) 
    RETURN u.username, u2.username, u3.username
    LIMIT 100

    MATCH (u:User)-[:FRIENDS]-(u2:User)-[:FRIENDS]-(u3:User) 
    RETURN u.username, u2.username, u3.username
    ORDER BY u.username, u2.username, u3.username
    LIMIT 100
        