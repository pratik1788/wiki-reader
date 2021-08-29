package com.ps.wikireader.event;

import com.ps.wikireader.enums.EvenetName;
import com.ps.wikireader.pojo.EventNotification;
import com.ps.wikireader.reader.RestResourceReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;

@Service
public class EventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    @Autowired
    private RestResourceReader restResourceReader;

    @Autowired
    private EventProducer eventProducer;

    @KafkaListener(topics = "${application.event.topic}", groupId = "${application.event.group-id}")
    public void consume(byte[] message,  @Header(KafkaHeaders.OFFSET) Integer offset) throws IOException {
        logger.info("Event received by reader");
        SpecificDatumReader<EventNotification> reader = new SpecificDatumReader<>(EventNotification.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        EventNotification eventNotification = reader.read(null, decoder);
        if(eventNotification.getEventName().equals(EvenetName.REQUEST_TO_START_READING_RESOURCE.getName())) {
            try {
                eventProducer.sendMessage(EventNotification.newBuilder()
                        .setFileName(eventNotification.getFileName())
                        .setEventName(EvenetName.RESOURCE_READING_STARTED.getName())
                        .setEventTimeStamp(Instant.now())
                        .build());
                boolean readStatus = restResourceReader.extractAndProcessData(eventNotification.getFileName());
                if (readStatus) {
                    eventProducer.sendMessage(EventNotification.newBuilder()
                            .setFileName(eventNotification.getFileName())
                            .setEventName(EvenetName.RESOURCE_READING_SUCCESSFUL.getName())
                            .setEventTimeStamp(Instant.now())
                            .build());
                }
            } catch (Exception e) {
                eventProducer.sendMessage(EventNotification.newBuilder()
                        .setFileName(eventNotification.getFileName())
                        .setEventName(EvenetName.RESOURCE_READING_FAILED.getName())
                        .setEventTimeStamp(Instant.now())
                        .setDetails(e.getMessage())
                        .build());
            }
        }
    }
}
