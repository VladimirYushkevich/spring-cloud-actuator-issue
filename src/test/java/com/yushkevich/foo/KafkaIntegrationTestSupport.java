package com.yushkevich.foo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yushkevich.foo.message.payload.Foo;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@ActiveProfiles("test")
public abstract class KafkaIntegrationTestSupport {

    public static final String PROCESSOR_TOPIC = "foo-processor";
    public static final String SINK_TOPIC = "foo-sink";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, false, 1,
            PROCESSOR_TOPIC, SINK_TOPIC);

    private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

    @BeforeClass
    public static void setup() {
        System.setProperty("spring.kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
        System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers", embeddedKafka.getBrokersAsString());
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("spring.cloud.stream.kafka.streams.binder.brokers");
    }

    protected void produceSynchronously(final List<Foo> foos, long timeout) {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new JsonSerde<>(Foo.class, new ObjectMapper()).serializer().getClass());
        KafkaTemplate<String, Foo> template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(senderProps), true);
        template.setDefaultTopic(PROCESSOR_TOPIC);

        foos.forEach(tour -> {
            template.send(MessageBuilder.withPayload(tour)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, tour.getId().toString())
                    .build());
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException ignore) {
            }
        });
    }
}
