package com.maxdemarzi;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.ArrayList;
import java.util.List;

public class GoodFriendExpander implements PathExpander {
    @Override
    public Iterable<Relationship> expand(Path path, BranchState branchState) {
        List<Relationship> rels = new ArrayList<>();
        for (Relationship r : path.endNode().getRelationships(RelationshipType.withName("FRIENDS"))) {
            if ((double)r.getProperty("weight") >= 0.80) {
                rels.add(r);
            }
        }
        return rels;
    }

    @Override
    public PathExpander reverse() {
        // Doesn't matter, do the same thing.
        return this;
    }
}
