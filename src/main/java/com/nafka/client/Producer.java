package com.nafka.client;

import com.nafka.service.TopicService;

public class Producer {
    private String name;
    private TopicService topicService;

    public Producer(String name) {
        this.name = name;
        this.topicService = TopicService.getInstance();
    }

    public <T> void publish(String topicName, T message){
        System.out.println(String.format("Producer: %s published message: %s to topic: %s",
            this.name,
            message,
            topicName));
        topicService.publish(topicName, message);
    }
}
