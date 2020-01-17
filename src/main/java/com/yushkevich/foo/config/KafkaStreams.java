package com.yushkevich.foo.config;

import com.yushkevich.foo.message.FooBarReducer;
import com.yushkevich.foo.message.FooBarTransformer;
import com.yushkevich.foo.message.payload.Foo;
import com.yushkevich.foo.message.payload.FooBar;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class KafkaStreams {

    public static final String FOO_MV = "foo-bar-mv";
    public static final String FOO_STORE = "foo-store";

    @Bean
    public Serde<FooBar> fooBarSerde() {
        return new JsonSerde<>(FooBar.class);
    }

    @Bean
    public StoreBuilder<KeyValueStore<String, FooBar>> fooStore(Serde<FooBar> fooBarSerde) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(FOO_STORE),
                Serdes.String(),
                fooBarSerde);
    }

    @Bean
    public Function<KStream<String, Foo>, KStream<String, FooBar>> process() {
        return foos -> foos.transform(FooBarTransformer::new, FOO_STORE);
    }

    @Bean
    public Consumer<KStream<String, FooBar>> reduce(Serde<FooBar> fooBarSerde) {
        return foos -> {
            foos.groupByKey(Grouped.with(Serdes.String(), fooBarSerde))
                    .reduce(new FooBarReducer(), Materialized.<String, FooBar, KeyValueStore<Bytes, byte[]>>as(FOO_MV)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(fooBarSerde)
                    );
        };
    }
}
