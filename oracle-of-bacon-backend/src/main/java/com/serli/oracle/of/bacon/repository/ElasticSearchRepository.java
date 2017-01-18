package com.serli.oracle.of.bacon.repository;

import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class ElasticSearchRepository {

    private final JestClient jestClient;

    public ElasticSearchRepository() {
        jestClient = createClient();

    }

    public static JestClient createClient() {
        JestClient jestClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());

        jestClient = factory.getObject();
        return jestClient;
    }

    public List<String> getActorsSuggests(String searchQuery) {
        try {
            String query = String.format(Locale.ROOT,
                    "{\n" +
                    "  \"query\": {\n" +
                    "    \"match\": {\n" +
                    "      \"name\": {\n" +
                    "        \"query\":     \"%s\",\n" +
                    "        \"fuzziness\": 1,\n" +
                    "        \"operator\":  \"and\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", searchQuery);
            Search suggestion = new Search.Builder(query)
                    .addIndex("bacon")
                    .addType("actors")
                    .build();
            SearchResult result = this.jestClient.execute(suggestion);
            if (result.isSucceeded()) {
                return result.getHits(JsonObject.class)
                        .stream()
                        .map(hit -> hit.source.get("name").getAsString())
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }


}
