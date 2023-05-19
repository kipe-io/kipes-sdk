package io.kipe.streams.kafka.examples.zomatorideranalysis.ingest;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.kipe.streams.kafka.examples.zomatorideranalysis.model.ZomatoOrder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ZomatoOrderDataIngestion {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZomatoOrderDataIngestion.class);
    private static final String CSV_FILE_PATH = "Rider-Info.csv";
    private static final String ZOMOTO_ORDER_TOPIC = "zomato.orders";

    public static void main(String[] args) {
        CsvMapper csvMapper = (CsvMapper) new CsvMapper().findAndRegisterModules();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();

        try (
                InputStream inputStream = ZomatoOrderDataIngestion.class.getClassLoader().getResourceAsStream(CSV_FILE_PATH);
                Producer<Long, ZomatoOrder> producer = new KafkaProducer<>(getProperties());
                MappingIterator<ZomatoOrder> iterator = csvMapper.readerFor(ZomatoOrder.class).with(schema).readValues(inputStream)
        ) {
            while (iterator.hasNext()) {
                ZomatoOrder zomatoOrder = iterator.next();
                ProducerRecord<Long, ZomatoOrder> event = new ProducerRecord<>(ZOMOTO_ORDER_TOPIC, zomatoOrder.getOrderId(), zomatoOrder);
                LOGGER.info("Producing event: {}", event);

                producer.send(event, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to send message with key {} due to error: ", zomatoOrder.getOrderId(), exception);
                    }
                });
            }
            producer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to read CSV file", e);
        } catch (Exception e) {
            LOGGER.error("Failed to send to Kafka", e);
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaJsonSerializer");
        return props;
    }
}
