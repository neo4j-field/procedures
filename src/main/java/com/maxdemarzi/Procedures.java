package com.maxdemarzi;

import com.maxdemarzi.results.LongResult;
import com.maxdemarzi.results.StringResult;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.HashSet;
import java.util.Iterator;
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

    @Procedure(name = "com.maxdemarzi.network.count", mode = Mode.READ)
    @Description("CALL com.maxdemarzi.network.count(String said)")
    public Stream<LongResult> networkCount(@Name("username") String username, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();

        Node user = db.findNode(Label.label("User"), "username", username);
        if (user == null) {
            return Stream.empty();
        } else {
            Long count = 0L;
            Iterator<Node> iterator;
            Node current;

            HashSet<Node> seen = new HashSet<>();
            HashSet<Node> nextA = new HashSet<>();
            HashSet<Node> nextB = new HashSet<>();

            seen.add(user);

            // First Hop
            for (Relationship r : user.getRelationships()) {
                nextB.add(r.getOtherNode(user));
            }

            for(int i = 1; i < distance; i++) {
                // next even Hop
                nextB.removeAll(seen);
                seen.addAll(nextB);
                nextA.clear();
                iterator = nextB.iterator();
                while (iterator.hasNext()) {
                    current = iterator.next();
                    for (Relationship r : current.getRelationships()) {
                        nextA.add(r.getOtherNode(current));
                    }
                }

                i++;
                if (i < distance) {
                    // next odd Hop
                    nextA.removeAll(seen);
                    seen.addAll(nextA);
                    nextB.clear();
                    iterator = nextA.iterator();
                    while (iterator.hasNext()) {
                        current = iterator.next();
                        for (Relationship r : current.getRelationships()) {
                            nextB.add(r.getOtherNode(current));
                        }
                    }

                }
                if((distance % 2) == 0) {
                    seen.addAll(nextA);
                } else {
                    seen.addAll(nextB);
                }

                // remove starting node
                seen.remove(user);
            }

            count = (long) seen.size();
            return Stream.of(new LongResult(count));
        }
    }


}
