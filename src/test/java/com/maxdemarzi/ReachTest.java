package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class ReachTest {
    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Procedures.class)
            .withFixture(MODEL_STATEMENT);

    @Test
    public void shouldReachAfter()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.reach.after($username1, $username2)",
                    parameters( "username1", "User-1", "username2", "User-6" ) );

            // Then I should get what I expect
            assertThat(result.list().size(), equalTo(1));
        }
    }

    @Test
    public void shouldReachEvaluator()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.reach.evaluator($username1, $username2)",
                    parameters( "username1", "User-1", "username2", "User-6" ) );

            // Then I should get what I expect
            assertThat(result.list().size(), equalTo(1));
        }
    }

    @Test
    public void shouldReachExpander()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.reach.expander($username1, $username2)",
                    parameters( "username1", "User-1", "username2", "User-6" ) );

            // Then I should get what I expect
            assertThat(result.list().size(), equalTo(1));
        }
    }

    private static final String MODEL_STATEMENT =
            "CREATE (n1:User { username:'User-1' })" +
                    "CREATE (n2:User { username:'User-2' })" +
                    "CREATE (n3:User { username:'User-3' })" +
                    "CREATE (n4:User { username:'User-4' })" +
                    "CREATE (n5:User { username:'User-5' })" +
                    "CREATE (n6:User { username:'User-6' })" +
                    "CREATE (n1)-[:FRIENDS {weight:0.8}]->(n3)" +
                    "CREATE (n2)-[:FRIENDS {weight:0.85}]->(n3)" +
                    "CREATE (n2)-[:FRIENDS {weight:0.7}]->(n1)" +
                    "CREATE (n3)-[:FRIENDS {weight:0.8}]->(n4)" +
                    "CREATE (n4)-[:FRIENDS {weight:0.9}]->(n5)" +
                    "CREATE (n6)-[:FRIENDS {weight:0.8}]->(n4)";
}
