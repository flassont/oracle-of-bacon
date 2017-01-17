package com.serli.oracle.of.bacon.repository;

import java.util.List;
import redis.clients.jedis.Jedis;

public class RedisRepository {

    private Jedis jedis;

    final private String SEARCHES = "searchs";

    public RedisRepository(){
        jedis = new Jedis("localhost",6379);
    }

    public void addSearch(String search) {
        jedis.lpush(SEARCHES, search);
    }

    public List<String> getLastTenSearches() {
        // TODO implement last 10 searchs
            jedis.ltrim(SEARCHES,0,9);
        return jedis.lrange(SEARCHES, 0, 9);
    }
}
