package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.google.gson.Gson;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.Flush;
import io.searchbox.indices.mapping.PutMapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
        PutMapping putMapping = new PutMapping.Builder(
                "bacon",
                "actors",
                "{ \"actors\" : " +
                        "{ \"properties\" : " +
                        "{ \"name\" : " +
                        "{\"type\" : \"string\"}" +
                        "}," +
                        "{ \"suggest\" :" +
                        "{\"type\" : \"completion\"}" +
                        "}" +
                        "}"
        ).build();
        client.execute(putMapping);
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex("bacon").defaultType("actors");
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            final Gson gson = new Gson();
            bufferedReader.lines()
                    .skip(1)
                    .parallel()
                    .collect(() -> new Bulk.Builder().defaultIndex("bacon").defaultType("actors"),
                            (builder, line) -> {
                                line = line.replaceAll("\"", "");
                                final Collection<String> suggestions = new ArrayList<>(Arrays.asList(line.replaceAll(",", "").split(" ")));
                                suggestions.add(String.join(" ", suggestions));
                                suggestions.add(line);
                                final String jsonSuggestions = gson.toJson(suggestions);
                                builder.addAction(new Index.Builder("{ \"name\": \"" + line + "\" , \"suggest\": { input : " + jsonSuggestions + "} }").build());
                            },
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
        } finally

        {
            client.execute(new Flush.Builder().build());
        }

        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
