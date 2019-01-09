package com.maxdemarzi;

import com.maxdemarzi.results.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.*;
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
    @Description("CALL com.maxdemarzi.network.count(String username, Long distance)")
    public Stream<LongResult> networkCount(@Name("username") String username, @Name(value = "distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();

        Node user = db.findNode(Label.label("User"), "username", username);
        if (user == null) {
            return Stream.empty();
        } else {
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

            for (int i = 1; i < distance; i++) {
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
            }

            if ((distance % 2) == 0) {
                seen.addAll(nextA);
            } else {
                seen.addAll(nextB);
            }

            // remove starting node
            seen.remove(user);

            return Stream.of(new LongResult((long) seen.size()));
        }
    }

    @Procedure("com.maxdemarzi.network.count2")
    @Description("com.maxdemarzi.network.count2(String username, Long distance)")
    public Stream<LongResult> networkCount2(@Name("username") String username, @Name(value = "distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();

        Node user = db.findNode(Label.label("User"), "username", username);
        if (user == null) {
            return Stream.empty();
        } else {
            HashSet<Node> nodes = new HashSet<>();
            TraversalDescription td = db.traversalDescription()
                    .depthFirst()
                    .expand(PathExpanders.allTypesAndDirections())
                    .evaluator(Evaluators.toDepth(distance.intValue()))
                    .uniqueness(Uniqueness.RELATIONSHIP_PATH);
            for (Path ignored : td.traverse(user)) {
                nodes.add(ignored.endNode());
            }

            // remove starting node
            nodes.remove(user);
            return Stream.of(new LongResult((long) nodes.size()));
        }
    }

    @Procedure("com.maxdemarzi.network.count3")
    @Description("com.maxdemarzi.network.count3(String username, Long distance)")
    public Stream<LongResult> networkCount3(@Name("username") String username, @Name(value = "distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();

        Node user = db.findNode(Label.label("User"), "username", username);
        if (user == null) {
            return Stream.empty();
        } else {
            TraversalDescription td = db.traversalDescription()
                    .breadthFirst()
                    .expand(PathExpanders.allTypesAndDirections())
                    .evaluator(Evaluators.toDepth(distance.intValue()))
                    .uniqueness(Uniqueness.NODE_GLOBAL);
            int count = 0;
            for (Path ignored : td.traverse(user)) {
                count++;
            }

            // remove starting node
            count--;
            return Stream.of(new LongResult((long) count));
        }
    }

    @Procedure("com.maxdemarzi.network.count4")
    @Description("com.maxdemarzi.network.count4(String username, Long distance)")
    public Stream<LongResult> networkCount4(@Name("username") String username, @Name(value = "distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();

        Node user = db.findNode(Label.label("User"), "username", username);
        if (user == null) {
            return Stream.empty();
        } else {
            Iterator<Long> iterator;
            Node current;
            Long currentId;
            HashSet<Long> seen = new HashSet<>();
            HashSet<Long> nextA = new HashSet<>();
            HashSet<Long> nextB = new HashSet<>();

            seen.add(user.getId());

            // First Hop
            for (Relationship r : user.getRelationships()) {
                nextB.add(r.getOtherNodeId(user.getId()));
            }

            for (int i = 1; i < distance; i++) {
                // next even Hop
                nextB.removeAll(seen);
                seen.addAll(nextB);
                nextA.clear();
                iterator = nextB.iterator();
                while (iterator.hasNext()) {
                    currentId = iterator.next();
                    current = db.getNodeById(currentId);
                    for (Relationship r : current.getRelationships()) {
                        nextA.add(r.getOtherNodeId(currentId));
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
                        currentId = iterator.next();
                        current = db.getNodeById(currentId);
                        for (Relationship r : current.getRelationships()) {
                            nextB.add(r.getOtherNodeId(currentId));
                        }
                    }
                }
            }

            if ((distance % 2) == 0) {
                seen.addAll(nextA);
            } else {
                seen.addAll(nextB);
            }

            // remove starting node
            seen.remove(user.getId());

            return Stream.of(new LongResult((long) seen.size()));
        }
    }

    @Procedure("com.maxdemarzi.network.count5")
    @Description("com.maxdemarzi.network.count5(String username, Long distance)")
    public Stream<LongResult> networkCount5(@Name("username") String username, @Name(value = "distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();

        Node user = db.findNode(Label.label("User"), "username", username);
        if (user == null) {
            return Stream.empty();
        } else {

            Node node;
            // Initialize bitmaps for iteration
            Roaring64NavigableMap seen = new Roaring64NavigableMap();
            Roaring64NavigableMap nextA = new Roaring64NavigableMap();
            Roaring64NavigableMap nextB = new Roaring64NavigableMap();
            long nodeId = user.getId();
            seen.add(nodeId);
            Iterator<Long> iterator;

            // First Hop
            for (Relationship r : user.getRelationships()) {
                nextB.add(r.getOtherNodeId(nodeId));
            }


            for (int i = 1; i < distance; i++) {
                // next even Hop
                nextB.andNot(seen);
                seen.or(nextB);
                nextA.clear();
                iterator = nextB.iterator();
                while (iterator.hasNext()) {
                    nodeId = iterator.next();
                    node = db.getNodeById(nodeId);
                    for (Relationship r : node.getRelationships()) {
                        nextA.add(r.getOtherNodeId(nodeId));
                    }
                }

                i++;
                if (i < distance) {
                    // next odd Hop
                    nextA.andNot(seen);
                    seen.or(nextA);
                    nextB.clear();
                    iterator = nextA.iterator();
                    while (iterator.hasNext()) {
                        nodeId = iterator.next();
                        node = db.getNodeById(nodeId);
                        for (Relationship r : node.getRelationships()) {
                            nextB.add(r.getOtherNodeId(nodeId));
                        }
                    }
                }
            }

            if ((distance % 2) == 0) {
                seen.or(nextA);
            } else {
                seen.or(nextB);
            }
            // remove starting node
            seen.removeLong(user.getId());

            return Stream.of(new LongResult(seen.getLongCardinality()));
        }
    }
    @Procedure("com.maxdemarzi.reach.after")
    @Description("com.maxdemarzi.reach.after(String username1, String username2)")
    public Stream<WeightedPathResult> reachAfter(@Name("username1") String username1, @Name("username2") String username2) {
        Node user1 = db.findNode(Label.label("User"), "username", username1);
        Node user2 = db.findNode(Label.label("User"), "username", username2);
        if (user1 == null || user2 == null) {
            return Stream.empty();
        }

        TraversalDescription leftSide = db.traversalDescription()
                .depthFirst()
                .expand(PathExpanders.allTypesAndDirections())
                .evaluator(Evaluators.toDepth(2))
                .uniqueness(Uniqueness.NODE_PATH);

        TraversalDescription rightSide = db.traversalDescription()
                .depthFirst()
                .expand(PathExpanders.allTypesAndDirections())
                .evaluator(Evaluators.toDepth(2))
                .uniqueness(Uniqueness.NODE_PATH);

        BidirectionalTraversalDescription bidirtd = db.bidirectionalTraversalDescription()
                .startSide(leftSide)
                .endSide(rightSide);

        ArrayList<WeightedPathResult> list = new ArrayList<>();

        outerloop:
        for (Path path : bidirtd.traverse(user1, user2)) {
            double weight = 0.0;
            for (Relationship r : path.relationships()) {
                double each = (double) r.getProperty("weight");
                if (each >= 0.80) {
                    weight += each;
                } else {
                    continue outerloop;
                }
            }
            list.add(new WeightedPathResult(path, weight / path.length()));
        }

        list.sort((o1, o2) -> Double.compare(o2.weight, o1.weight));

        return list.stream().limit(100);
    }

    @Procedure("com.maxdemarzi.reach.evaluator")
    @Description("com.maxdemarzi.reach.evaluator(String username1, String username2)")
    public Stream<WeightedPathResult> reachEvaluator(@Name("username1") String username1, @Name("username2") String username2) {
        Node user1 = db.findNode(Label.label("User"), "username", username1);
        Node user2 = db.findNode(Label.label("User"), "username", username2);
        if (user1 == null || user2 == null) {
            return Stream.empty();
        }

        TraversalDescription eachSide = db.traversalDescription()
                .breadthFirst()
                .expand(PathExpanders.allTypesAndDirections())
                .evaluator(Evaluators.toDepth(2))
                .evaluator(new GoodFriendEvaluator())
                .uniqueness(Uniqueness.NODE_PATH);

        BidirectionalTraversalDescription bidirtd = db.bidirectionalTraversalDescription()
                .mirroredSides(eachSide);

        ArrayList<WeightedPathResult> list = new ArrayList<>();

        for (Path path : bidirtd.traverse(user1, user2)) {
            double weight = 0.0;
            for (Relationship r : path.relationships()) {
                weight += (double) r.getProperty("weight");
            }
            list.add(new WeightedPathResult(path, weight / path.length()));
        }

        list.sort((o1, o2) -> Double.compare(o2.weight, o1.weight));

        return list.stream().limit(100);
    }

    @Procedure("com.maxdemarzi.reach.expander")
    @Description("com.maxdemarzi.reach.expander(String username1, String username2)")
    public Stream<WeightedPathResult> reachExpander(@Name("username1") String username1, @Name("username2") String username2) {
        Node user1 = db.findNode(Label.label("User"), "username", username1);
        Node user2 = db.findNode(Label.label("User"), "username", username2);
        if (user1 == null || user2 == null) {
            return Stream.empty();
        }

        TraversalDescription eachSide = db.traversalDescription()
                .breadthFirst()
                .expand(new GoodFriendExpander())
                .evaluator(Evaluators.toDepth(2))
                .uniqueness(Uniqueness.NODE_PATH);

        BidirectionalTraversalDescription bidirtd = db.bidirectionalTraversalDescription()
                .mirroredSides(eachSide);

        ArrayList<WeightedPathResult> list = new ArrayList<>();

        for (Path path : bidirtd.traverse(user1, user2)) {
            double weight = 0.0;
            for (Relationship r : path.relationships()) {
                weight += (double) r.getProperty("weight");
            }
            list.add(new WeightedPathResult(path, weight / path.length()));
        }

        list.sort((o1, o2) -> Double.compare(o2.weight, o1.weight));

        return list.stream().limit(100);
    }

    @Procedure("com.maxdemarzi.reach.both")
    @Description("com.maxdemarzi.reach.both(String username1, String username2)")
    public Stream<WeightedPathResult> reachBoth(@Name("username1") String username1, @Name("username2") String username2) {
        Node user1 = db.findNode(Label.label("User"), "username", username1);
        Node user2 = db.findNode(Label.label("User"), "username", username2);
        if (user1 == null || user2 == null) {
            return Stream.empty();
        }

        Comparator<WeightedPathResult> comparator = (o1, o2) -> Double.compare(o2.weight, o1.weight);

        PriorityQueue<WeightedPathResult> paths = new PriorityQueue<>(100, comparator);

        TraversalDescription eachSide = db.traversalDescription()
                .breadthFirst()
                .expand(new GoodFriendExpander())
                .evaluator(Evaluators.toDepth(2))
                .uniqueness(Uniqueness.NODE_PATH);

        BidirectionalTraversalDescription bidirtd = db.bidirectionalTraversalDescription()
                .mirroredSides(eachSide)
                .collisionEvaluator(new TooManyFriendsEvaluator(paths));

        int count = 0;
        for (Path ignore : bidirtd.traverse(user1, user2)) {
            count++;
        }

        return paths.stream();
    }

}
