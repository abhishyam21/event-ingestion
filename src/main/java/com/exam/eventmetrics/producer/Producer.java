package com.exam.eventmetrics.producer;


import com.exam.eventmetrics.helper.ExternalApiInvoker;
import com.exam.eventmetrics.pojoentites.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class Producer {
    @Autowired
    private KafkaTemplate<String, Event> kafkaTemplate;

    @Autowired
    private ExternalApiInvoker externalApiInvoker;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${application.github.url}")
    private String githubUrl;

    @Scheduled(fixedRate = 60000)
    public void sendMessage() {
        String bodyForEvents = externalApiInvoker.callExternalApi(githubUrl);
        List<Event> events = convertStringToObject(bodyForEvents);
        assert events != null;
        events.forEach(event -> {
            log.info("Pushing event with id {} to kafka broker", event.getId());
            Message<Event> message = MessageBuilder
                    .withPayload(event)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .build();
            ListenableFuture<SendResult<String, Event>> future = kafkaTemplate.send(message);
            if (future.isDone()) {
                log.info("Successfully published event {}", event.getId());
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

    }

    private List<Event> convertStringToObject(String body) {
        ObjectMapper oMapper = new ObjectMapper();
        try {
            return Arrays.asList(oMapper.readValue(body, Event[].class));
        } catch (JsonProcessingException e) {
            log.error("Error while converting the json string to object");
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] serialize(final Object obj) {
        return org.apache.commons.lang3.SerializationUtils.serialize((Serializable) obj);
    }
}
