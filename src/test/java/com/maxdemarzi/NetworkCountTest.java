package com.maxdemarzi;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class NetworkCountTest {

    private static ServerControls neo4j;

    @BeforeAll
    static void startNeo4j() {
        neo4j = TestServerBuilders.newInProcessBuilder()
            .withProcedure(Procedures.class)
            .withFixture(MODEL_STATEMENT)
            .newServer();
    }

    @AfterAll
    static void stopNeo4j() {
        neo4j.close();
    }

    @Test
    void shouldNetworkCount()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.network.count($username, $distance)",
                    parameters( "username", "User-1", "distance", 3 ) );

            // Then I should get what I expect
            assertThat(result.single().get("value").asInt()).isEqualTo(5);
        }
    }

    @Test
    void shouldNetworkCount2()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.network.count2($username, $distance)",
                    parameters( "username", "User-1", "distance", 3 ) );

            // Then I should get what I expect
            assertThat(result.single().get("value").asLong()).isEqualTo(5L);
        }
    }

    @Test
    void shouldNetworkCount3()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.network.count3($username, $distance)",
                    parameters( "username", "User-1", "distance", 3 ) );

            // Then I should get what I expect
            assertThat(result.single().get("value").asLong()).isEqualTo(5L);
        }
    }

    @Test
    void shouldNetworkCount4()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.network.count4($username, $distance)",
                    parameters( "username", "User-1", "distance", 3 ) );

            // Then I should get what I expect
            assertThat(result.single().get("value").asLong()).isEqualTo(5L);
        }
    }

    @Test
    void shouldNetworkCount5()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.network.count5($username, $distance)",
                    parameters( "username", "User-1", "distance", 3 ) );

            // Then I should get what I expect
            assertThat(result.single().get("value").asLong()).isEqualTo(5L);
        }
    }
    private static final String MODEL_STATEMENT =
            "CREATE (n1:User { username:'User-1' })" +
                    "CREATE (n2:User { username:'User-2' })" +
                    "CREATE (n3:User { username:'User-3' })" +
                    "CREATE (n4:User { username:'User-4' })" +
                    "CREATE (n5:User { username:'User-5' })" +
                    "CREATE (n6:User { username:'User-6' })" +
                    "CREATE (n1)-[:FRIENDS]->(n3)" +
                    "CREATE (n2)-[:FRIENDS]->(n3)" +
                    "CREATE (n2)-[:FRIENDS]->(n1)" +
                    "CREATE (n3)-[:FRIENDS]->(n4)" +
                    "CREATE (n4)-[:FRIENDS]->(n5)" +
                    "CREATE (n6)-[:FRIENDS]->(n4)";
}
