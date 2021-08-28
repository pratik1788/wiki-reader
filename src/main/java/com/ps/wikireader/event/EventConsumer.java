package com.ps.wikireader.event;

import com.ps.wikireader.pojo.EventNotification;
import com.ps.wikireader.pojo.ExtractionRequest;
import com.ps.wikireader.reader.RestResourceReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import com.ps.wikireader.enums.EvenetName;

import java.io.IOException;

public class EventConsumer {

    @Autowired
    private RestResourceReader restResourceReader;

    @Autowired
    private EventProducer eventProducer;

    @KafkaListener(topics = "${application.listener.topic}", groupId = "${spring.kafka.consumer.groupId}")
    public void consume(byte[] message,  @Header(KafkaHeaders.OFFSET) Integer offset) throws IOException {
        SpecificDatumReader<EventNotification> reader = new SpecificDatumReader<>(EventNotification.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        EventNotification eventNotification = reader.read(null, decoder);
        if(eventNotification.getEventName().equals(EvenetName.START_EXTRACTION.getName())) {
            try {
                boolean readStatus = restResourceReader.extractAndProcessData(ExtractionRequest.builder().filenameToExtract(eventNotification.getFileName()).build());
                if (readStatus) {
                    eventProducer.sendMessage(EventNotification.newBuilder()
                            .setFileName(eventNotification.getFileName())
                            .setEventName(EvenetName.EXTRATCION_SUCCESSFUL.getName())
                            .build());
                }
            } catch (Exception e) {
                eventProducer.sendMessage(EventNotification.newBuilder()
                        .setFileName(eventNotification.getFileName())
                        .setEventName(EvenetName.EXTRATCION_FAILED.getName())
                        .build());
            }
        }
    }
}
