package com.nafka.client;

import com.nafka.model.IConsumer;
import com.nafka.service.TopicService;
import java.util.UUID;
import lombok.Getter;

@Getter
public class Consumer implements IConsumer {
    private String ID = UUID.randomUUID().toString();
    private String name;
    private TopicService topicService = TopicService.getInstance();

    public Consumer(String name) {
        this.name = name;
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public void update(String topicName) {
        System.out.println(String.format("Consumer: %s read message: %s from topic: %s",
            this.name,
            topicService.readMessage(topicName, this).getMessage(),
            topicName));
    }

}
