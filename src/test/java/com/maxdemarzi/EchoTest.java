package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class EchoTest {
    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Procedures.class);

    @Test
    public void shouldEcho()
    {
        // In a try-block, to make sure we close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build().withoutEncryption().toConfig() ) )
        {

            // Given I've started Neo4j with the procedure
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // When I use the procedure
            StatementResult result = session.run( "CALL com.maxdemarzi.echo($something)",
                    parameters( "something", "It works!" ) );

            // Then I should get what I expect
            assertThat(result.single().get("value").asString(), equalTo("It works!"));
        }
    }

}
