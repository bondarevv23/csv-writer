package com.github.bondarevv23;


import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;
import java.util.stream.Collectors;

public class RedisMap implements Map<String, String> {
    private static final String ZERO_CURSOR = "0";

    private final JedisPooled jedis;

    public RedisMap(JedisPooled jedis) {
        this.jedis = jedis;
    }

    @Override
    public int size() {
        return (int) jedis.dbSize();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return jedis.exists(key.toString());
    }

    @Override
    public boolean containsValue(Object value) {
        return values().contains(value);
    }

    @Override
    public String get(Object key) {
        return jedis.get(key.toString());
    }

    @Override
    public String put(String key, String value) {
        return jedis.set(key, value);
    }

    @Override
    public String remove(Object key) {
        String value = jedis.get(key.toString());
        if (value != null) {
            jedis.del(key.toString());
        }
        return value;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        m.forEach(jedis::set);
    }

    @Override
    public void clear() {
        jedis.flushAll();
    }

    @Override
    public Set<String> keySet() {
        Set<String> set = new HashSet<>();
        String cursor = ZERO_CURSOR;
        do {
            ScanResult<String> scanResult = jedis.scan(cursor);
            cursor = scanResult.getCursor();
            set.addAll(scanResult.getResult());
        } while (!cursor.equals(ZERO_CURSOR));
        return set;
    }

    @Override
    public Collection<String> values() {
        return keySet().stream().map(this::get).collect(Collectors.toList());
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return keySet().stream()
                .map(key -> new AbstractMap.SimpleEntry<>(key, jedis.get(key)))
                .collect(Collectors.toSet());
    }
}
