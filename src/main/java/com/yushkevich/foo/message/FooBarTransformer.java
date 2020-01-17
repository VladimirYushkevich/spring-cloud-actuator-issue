package com.yushkevich.foo.message;

import com.yushkevich.foo.message.payload.Foo;
import com.yushkevich.foo.message.payload.FooBar;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

import static com.yushkevich.foo.config.KafkaStreams.FOO_STORE;

@Slf4j
public class FooBarTransformer implements Transformer<String, Foo, KeyValue<String, FooBar>> {

    private KeyValueStore<String, FooBar> fooStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        fooStore = (KeyValueStore<String, FooBar>) context.getStateStore(FOO_STORE);
    }

    @Override
    public KeyValue<String, FooBar> transform(String key, Foo value) {
        log.info("::map backup {}->{}", key, value);

        var id = value.getId();
        var originalId = value.getOriginalId() == null ? id : value.getOriginalId();
        var maybeFooBar = Optional.ofNullable(fooStore.get(originalId.toString()));
        var uniqueId = maybeFooBar.map(FooBar::getUniqueId).orElse(id);

        var tourBackup = FooBar.fooBarBuilder()
                .id(id)
                .originalId(originalId)
                .uniqueId(uniqueId)
                .value(value.getValue())
                .build();
        fooStore.put(id.toString(), tourBackup);

        return KeyValue.pair(uniqueId.toString(), tourBackup);
    }

    @Override
    public void close() {
        // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }
}
