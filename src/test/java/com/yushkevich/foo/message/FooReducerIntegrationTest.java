package com.yushkevich.foo.message;

import com.yushkevich.foo.KafkaIntegrationTestSupport;
import com.yushkevich.foo.message.payload.Foo;
import com.yushkevich.foo.message.payload.FooBar;
import com.yushkevich.foo.service.FooService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FooReducerIntegrationTest extends KafkaIntegrationTestSupport {
    @Autowired
    private FooService lookup;

    private List<Foo> foos = List.of(
            Foo.builder().id(1L).originalId(null).value("A").build(),
            Foo.builder().id(2L).originalId(1L).value("B").build(),
            Foo.builder().id(4L).originalId(null).value("B new").build(),
            Foo.builder().id(3L).originalId(2L).value("C").build(),
            Foo.builder().id(5L).originalId(4L).value("C new").build()
    );

    @Test
    public void testFoo() throws Exception {
        produceSynchronously(foos, 0);

        //allow to consume
        Thread.sleep(5000);

        // last (backup) value for original key
        FooBar actual = lookup.findFooBar("1").get();
        assertThat(actual.getId(), is(3L));
        assertThat(actual.getOriginalId(), is(2L));
        assertThat(actual.getUniqueId(), is(1L));
        assertThat(actual.getValue(), is("C"));
        // another tour
        assertThat(lookup.findFooBar("4").get().getValue(), is("C new"));
    }

}
