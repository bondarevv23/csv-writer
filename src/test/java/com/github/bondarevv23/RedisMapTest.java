package com.github.bondarevv23;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import redis.clients.jedis.JedisPooled;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class RedisMapTest {
    private final Map<String, String> redisMap = new RedisMap(
            new JedisPooled("localhost", 6379, "default", "password")
    );

    @BeforeEach
    void clear() {
        redisMap.clear();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 10, 100, 200})
    void whenAddXStrings_thenMapSizeEqualToX(int n) {
        // given

        // when
        putRange(n);

        // then
        assertThat(redisMap.size()).isEqualTo(n);
    }

    @Test
    void whenRedisMapIsEmpty_thenSizeEqualTo0() {
        // given

        // when

        // then
        assertThat(redisMap.size()).isEqualTo(0);
    }

    @Test
    void whenRedisIsEmpty_thenIsEmptyFunctionReturnsTrue() {
        // given

        // when

        // then
        assertThat(redisMap).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 5})
    void whenAddAnyEntries_thenRedisMapIsNotEmpty(int n) {
        // given

        // when
        putRange(n);

        // then
        assertThat(redisMap).isNotEmpty();
    }

    @Test
    void whenPutNewEntry_thenMapContainsKey() {
        // given
        String key = "key";
        String value = "value";

        // when
        redisMap.put(key, value);

        // then
        assertThat(redisMap).containsKey(key);
    }

    @Test
    void whenWrongKey_thenMapDoesntContainIt() {
        // given
        String wrongKey = "wrongKey";
        String key = "key";
        String value = "value";

        // when
        redisMap.put(key, value);

        // then
        assertThat(redisMap).doesNotContainKey(wrongKey);
    }

    @Test
    void whenDeleteKey_thenMapDoesntContainIt() {
        // given
        String key = "key";
        String value = "value";
        redisMap.put(key, value);

        // when
        redisMap.remove(key);

        // then
        assertThat(redisMap).doesNotContainKey(key);
    }

    @Test
    void whenAddNewEntry_thenRedisMapContainsValue() {
        // given
        String key = "key";
        String value = "value";

        // when
        redisMap.put(key, value);

        // then
        assertThat(redisMap).containsValue(value);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 5, 10})
    void whenMapDoesntContainValue_thenContainsValueFunctionReturnsFalse(int n) {
        // given
        String wrongValue = "-1";

        // when
        putRange(n);

        // then
        assertThat(redisMap).doesNotContainValue(wrongValue);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10})
    void whenGetByExistingKey_thenCorrespondingValueReturns(int n) {
        // given
        putRange(n);

        // when

        // then
        stringRange(n).forEach(s -> {
            assertThat(redisMap.get(s)).isEqualTo(s);
        });
    }

    @Test
    void whenGetByWrongKey_thenNullReturns() {
        // given
        String wrongKey = "-1";

        // when

        // then
        assertThat(redisMap.get(wrongKey)).isNull();
    }

    @Test
    void whenUpdateExistedEntry_thenGetReturnsNewValue() {
        // given
        String key = "key";
        String value = "value";
        redisMap.put(key, value);
        String newValue = "newValue";

        // when
        redisMap.put(key, newValue);

        // then
        assertThat(redisMap.get(key)).isEqualTo(newValue);
    }

    @Test
    void whenPutValueByKey_thenValueAccessibleByKey() {
        // given
        String key = "key";
        String value = "value";

        // when
        redisMap.put(key, value);

        // then
        assertThat(redisMap.get(key)).isEqualTo(value);
    }

    @Test
    void whenPutNewValueByExistingKey_thenGetReturnsNewValue() {
        // given
        String key = "key";
        String value = "value";
        redisMap.put(key, value);
        String newValue = "newValue";

        // when
        redisMap.put(key, newValue);

        // then
        assertThat(redisMap.get(key)).isEqualTo(newValue);
    }

    @Test
    void whenRemoveExistingKey_thenKeyRemoved() {
        // given
        String key = "key";
        String value = "value";
        redisMap.put(key, value);

        // when
        redisMap.remove(key);

        // then
        assertThat(redisMap.size()).isEqualTo(0);
        assertThat(redisMap.get(key)).isNull();
        assertThat(redisMap).doesNotContainKey(key);
        assertThat(redisMap).doesNotContainValue(value);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10})
    void whenRemoveByWrongKey_thenNothingHappens(int n) {
        // given
        String wrongKey = "wrongKey";
        putRange(n);

        // when
        String removed = redisMap.remove(wrongKey);

        // then
        assertThat(removed).isNull();
        assertThat(redisMap.keySet()).containsAll(stringRange(n));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 7})
    void whenPutAllEntries_thenAllEntriesAdded(int n) {
        // given
        Map<String, String> map = stringRange(n).stream().collect(Collectors.toMap(s -> s, s -> s));

        // when
        redisMap.putAll(map);

        // then
        assertThat(redisMap).containsExactlyEntriesOf(map);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 10, 50})
    void whenClear_thenMapIsEmpty(int n) {
        // given
        putRange(n);

        // when
        redisMap.clear();

        // then
        assertThat(redisMap).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 10, 50})
    void whenPutManyEntries_thenKeySetContainsAllKeysAndOnlyThem(int n) {
        // given
        putRange(n);

        // when
        Set<String> keys = redisMap.keySet();

        // then
        assertThat(keys).size().isEqualTo(n);
        assertThat(keys).containsAnyElementsOf(stringRange(n));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 50})
    void whenPutWithDuplicatesKeys_thenKeySetContainsAllKeysAndOnlyThem(int n) {
        // given
        putRange(n);
        ThreadLocalRandom.current().ints(0, n).limit(n).forEach(this::putRange);

        // when
        Set<String> keys = redisMap.keySet();

        // then
        assertThat(keys).size().isEqualTo(n);
        assertThat(keys).containsAnyElementsOf(stringRange(n));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 20})
    void whenPutManyEntriesAndGetValues_thenAllValuesReturned(int n) {
        // given
        IntStream.range(0, n).forEach(i -> redisMap.put(String.valueOf(i), String.valueOf(i)));
        IntStream.range(n, 2 * n).forEach(i -> redisMap.put(String.valueOf(i), String.valueOf(i - n)));
        IntStream.range(2 * n, 3 * n).forEach(i -> redisMap.put(String.valueOf(i), String.valueOf(i - 2 * n)));

        // when
        Collection<String> values = redisMap.values();

        // then
        Map<String, Long> collectedValues = values.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        assertThat(collectedValues).hasSize(n);
        assertThat(collectedValues.values()).containsOnly(3L);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10, 100})
    void whenPutManyEntriesToTheRedisMapAndToHashMap_thenTheirEntriesAreEqual(int n) {
        // given
        putRange(n);

        // when
        Map<String, String> map = new HashMap<>();
        stringRange(n).forEach(s -> map.put(s, s));

        // then
        assertThat(redisMap).containsExactlyInAnyOrderEntriesOf(map);
    }

    private void putRange(int n) {
        stringRange(n).forEach(s -> redisMap.put(s, s));
    }

    private Collection<String> stringRange(int n) {
        return  IntStream.range(0, n).mapToObj(String::valueOf).toList();
    }
}
