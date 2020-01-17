package com.yushkevich.foo.message;

import com.yushkevich.foo.message.payload.FooBar;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Reducer;

@Slf4j
public class FooBarReducer implements Reducer<FooBar> {
    public FooBar apply(final FooBar prevValue, final FooBar nextValue) {
        FooBar reducedValue = com.yushkevich.foo.message.payload.FooBar.fooBarBuilder()
                .id(nextValue.getId())
                .originalId(nextValue.getOriginalId())
                .value(nextValue.getValue())
                .uniqueId(prevValue.getUniqueId())
                .build();
        log.info("::reducing old->new->res:{}->{}->{}", prevValue, nextValue, reducedValue);
        return reducedValue;
    }
}
