package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.Flush;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();

        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader.lines()
                    .skip(1)
                    .parallel()
                    .collect(() -> new Bulk.Builder().defaultIndex("bacon").defaultType("actors"),
                            (builder, line) -> builder.addAction(new Index.Builder(String.format(Locale.ROOT, "{ \"name\": %s}", line)).build()),
                            (builder, builder2) -> {
                                try {
                                    client.execute(builder.build());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                                try {
                                    client.execute(builder2.build());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });
        } finally {
            client.execute(new Flush.Builder().build());
        }

        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
