package com.example.mcre.kafka;

import com.example.mcre.dto.MessageDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class MyKafkaListenerTest {

    @Autowired
    private MyKafkaListener kafkaListener;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @SneakyThrows
    @Test
    void onMessage_WhenRecordInterceptorThrowsException_ShouldRereadMessage() {
        var messageDto = MessageDto.builder()
                .id(1)
                .content("test")
                .build();
        kafkaTemplate.send("testTopic", objectMapper.writeValueAsString(messageDto));
        kafkaTemplate.flush();

        var await = kafkaListener.getLatch().await(10, TimeUnit.SECONDS);
        assertThat(await).isTrue();
    }
}