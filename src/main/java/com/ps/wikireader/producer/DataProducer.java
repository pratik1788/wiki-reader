package com.ps.wikireader.producer;

import com.ps.wikireader.pojo.WikiData;
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
public class DataProducer {
    private static final Logger logger = LoggerFactory.getLogger(DataProducer.class);
    @Value("${application.data.producer.topic}")
    String topic;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    public void sendMessage(WikiData message, String messageKey) throws IOException {
        logger.debug("producing message on topic {} with message {}",topic,message);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<WikiData> writer = new SpecificDatumWriter<>(WikiData.getClassSchema());
        writer.write(message, encoder);
        encoder.flush();
        out.close();
        this.kafkaTemplate.send(topic, messageKey,out.toByteArray());
    }

}
