package com.maxdemarzi;

import com.maxdemarzi.results.WeightedPathResult;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

import java.util.PriorityQueue;

public class TooManyFriendsEvaluator implements Evaluator {
    private PriorityQueue<WeightedPathResult> paths;

    TooManyFriendsEvaluator(PriorityQueue<WeightedPathResult> paths) {
        this.paths = paths;
    }

    @Override
    public Evaluation evaluate(Path path) {
        double weight = 0.0;
        for (Relationship r : path.relationships()) {
            weight += (double) r.getProperty("weight");
        }

        if (paths.size() < 100) {
            paths.add(new WeightedPathResult(path, weight / path.length()));
        } else if (paths.peek().weight < weight/path.length()) {
            paths.poll();
            paths.add(new WeightedPathResult(path, weight / path.length()));
        }

        return Evaluation.EXCLUDE_AND_PRUNE;
    }
}
