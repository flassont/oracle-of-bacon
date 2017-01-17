package com.serli.oracle.of.bacon.repository;


import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;


public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "0rph@nBl@ck"));
    }

    public List<?> getConnectionsToKevinBacon(String actorName) {
        // TODO implement Oracle of Bacon
        try (final Session session = driver.session()) {
            final Path match = session.run(
                    "MATCH (bacon:Actor {name:\"Bacon, Kevin (I)\"}),\n" +
                            "      (actor:Actor {name: {actorName}}),\n" +
                            "      p = shortestPath((bacon)-[:PLAYED_IN*]-(actor))\n" +
                            "RETURN p",
                    Values.parameters("actorName", actorName))
                    .single()
                    .get("p").asPath();
            final LinkedList<GraphItem> items = new LinkedList<>();
            match.nodes().forEach(node -> items.addFirst(Neo4JRepository.toGraphItem(node)));
            match.relationships().forEach(relation -> items.addFirst(Neo4JRepository.toGraphItem(relation)));
            return items;
        }
    }

    private static GraphItem toGraphItem(final Node node) {
        final String label = Iterables.get(node.labels(), 0);
        final String valueLabel;
        if (label.equalsIgnoreCase("Actor")) {
            valueLabel = "name";
        } else {
            valueLabel = "title";
        }

        return new GraphNode(node.id(), node.get(valueLabel).asString(), label);
    }

    private static GraphItem toGraphItem(final Relationship rel) {
        return new GraphEdge(rel.id(), rel.startNodeId(), rel.endNodeId(), rel.type());
    }

    private static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
