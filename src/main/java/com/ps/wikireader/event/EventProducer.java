package com.ps.wikireader.event;

import com.ps.wikireader.pojo.EventNotification;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
public class EventProducer {
    private static final Logger logger = LoggerFactory.getLogger(EventProducer.class);
    @Value("${application.event.topic}")
    String topic;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public void sendMessage(EventNotification notification) throws IOException {
        logger.debug("producing message on topic {} with message {}",topic,notification);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<EventNotification> writer = new SpecificDatumWriter<>(EventNotification.getClassSchema());
        writer.write(notification, encoder);
        encoder.flush();
        out.close();
        this.kafkaTemplate.send(topic, out.toByteArray());
    }
}
