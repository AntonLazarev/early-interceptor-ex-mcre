package com.example.mcre.kafka;

import com.example.mcre.dto.MessageDto;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
@Getter
public class MyKafkaListener {

    private final CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(topics = "testTopic", groupId = "testGroup", containerFactory = "myConcurrentKafkaListenerContainerFactory")
    public void onMessage(MessageDto messageDto) {
        log.debug("received msg: {}", messageDto);
        latch.countDown();
    }
}