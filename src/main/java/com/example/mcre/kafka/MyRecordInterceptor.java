package com.example.mcre.kafka;

import com.example.mcre.ex.MyException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

@Slf4j
public class MyRecordInterceptor<K, V> implements RecordInterceptor<K, V> {

    private int counter = 0;

    @Override
    public ConsumerRecord<K, V> intercept(@NonNull ConsumerRecord<K, V> consumerRecord) {
        if (counter < 3) {
            counter++;
            throw new MyException("test exception");
        }
        return consumerRecord;
    }
}