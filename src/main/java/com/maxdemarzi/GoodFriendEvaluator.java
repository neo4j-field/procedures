package com.maxdemarzi;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class GoodFriendEvaluator implements Evaluator {
    @Override
    public Evaluation evaluate(Path path) {
        for (Relationship r : path.relationships()) {
            if ((double)r.getProperty("weight") < 0.80) {
                return Evaluation.EXCLUDE_AND_PRUNE;
            }
        }
        return Evaluation.INCLUDE_AND_CONTINUE;
    }
}
