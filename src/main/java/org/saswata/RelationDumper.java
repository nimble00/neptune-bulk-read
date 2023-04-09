package org.saswata;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.inside;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.where;

public class RelationDumper {

    private static final Integer ORDER_WINDOW = 5184000;  // TWO MONTHS EPOCH TIME;

    private static final String TS = "ts";
    private static final String[] EDGES = List.of("has_email", "has_ipadd", "has_phone", "has_devid", "has_token", "has_fubid", "has_ubid")
            .toArray(new String[7]);
    private static final Integer NUM_SAMPLE_NEIGHBORS = 15;

    private final GraphTraversalSource g;
    private final Path parent;

    public RelationDumper(GraphTraversalSource g, String folder) throws IOException {
        this.g = g;
        this.parent = Files.createDirectories(Paths.get(folder));
    }

    public void process(String line) throws IOException {
        String[] items = line.split(",");
        if (items.length != 2) throw new IllegalArgumentException("Malformed input line " + line);

        String id = items[0];
        String ts = items[1];

        queryChildAccounts(id, Integer.parseInt(ts));
    }

    private List<org.apache.tinkerpop.gremlin.process.traversal.Path> queryChildAccounts(String vid, Integer src_timestamp) {
        return g.with("evaluationTimeout", 5000).V(vid).inV().as("hop_1")
                .local(out(EDGES).has(TS, inside(src_timestamp - ORDER_WINDOW, src_timestamp))
                        .limit(NUM_SAMPLE_NEIGHBORS).as("hop_2"))
                .optional(
                        in(EDGES).as("hop_3").local(out(EDGES).union(
                                where(lt("hop_2")).by(TS).has(TS, gt(src_timestamp - ORDER_WINDOW)).limit(
                                        NUM_SAMPLE_NEIGHBORS).as("hop_4f"),
                                where(gt("hop_2")).by(TS).has(TS, inside(src_timestamp - ORDER_WINDOW, src_timestamp)).limit(
                                        NUM_SAMPLE_NEIGHBORS).as("hop_4b")
                        ))
                ).path().by(T.id).from("hop_1").dedup().toList();
    }
}
