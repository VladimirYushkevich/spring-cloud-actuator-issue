package com.yushkevich.foo.service;

import com.yushkevich.foo.message.payload.FooBar;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.StreamSupport;

import static com.yushkevich.foo.config.KafkaStreams.FOO_MV;

@Component
@RequiredArgsConstructor
@Slf4j
public class FooService {

    private final InteractiveQueryService interactiveQueryService;

    public Optional<FooBar> findFooBar(String key) {
        ReadOnlyKeyValueStore<String, FooBar> store;
        try {
            store = waitUntilStoreIsQueryable(interactiveQueryService, FOO_MV, QueryableStoreTypes.keyValueStore());
            var storedFooBar = store.get(key);
            log.info("::found {} for key={}, total={}", storedFooBar, key, StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(store.all(), Spliterator.ORDERED), false)
                    .count());
            return Optional.ofNullable(storedFooBar);
        } catch (Exception e) {
            log.error("can't find in store by id={}, reason: ", key, e);
        }
        return Optional.empty();
    }

    public List<FooBar> findAllFooBar() {
        ReadOnlyKeyValueStore<String, FooBar> store;
        List<FooBar> all = new ArrayList<>();
        try {
            store = waitUntilStoreIsQueryable(interactiveQueryService, FOO_MV, QueryableStoreTypes.keyValueStore());
            var iterator = store.all();
            while (iterator.hasNext()) {
                KeyValue<String, FooBar> next = iterator.next();
                all.add(next.value);
                log.info("::next {}->{}", next.key, next.value);
            }
        } catch (Exception e) {
            log.error("can't find in store, reason: ", e);
        }
        return all;

    }

    private static <T> T waitUntilStoreIsQueryable(InteractiveQueryService interactiveQueryService, String storeName,
                                                   QueryableStoreType<T> queryableStoreType) throws InterruptedException {
        while (true) {
            try {
                return interactiveQueryService.getQueryableStore(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                log.info("store not yet ready for querying. Retry.....");
                Thread.sleep(100);
            }
        }
    }
}
