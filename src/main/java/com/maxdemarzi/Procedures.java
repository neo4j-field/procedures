package com.maxdemarzi;

import com.maxdemarzi.results.StringResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.stream.Stream;


public class Procedures {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Procedure(name = "com.maxdemarzi.echo", mode = Mode.READ)
    @Description("CALL com.maxdemarzi.echo(String said)")
    public Stream<StringResult> echo(@Name("said") String said) {
        return Stream.of(new StringResult(said));
    }

}
