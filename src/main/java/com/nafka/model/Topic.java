package com.nafka.model;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

public class Topic {
    @Getter
    private String name;
    private Map<String, IConsumer> consumerList;
    private Map<String, String> consumerLastMessageId;

    public Topic(String name) {
        this.name = name;
        this.consumerList = new HashMap<>();
        consumerLastMessageId = new HashMap<>();
    }

    public void registerConsumer(IConsumer consumer){
        consumerList.put(consumer.getId(), consumer);
    }

    public void unregisterConsumer(IConsumer consumer){
       consumerList.remove(consumer.getId());
    }

    public void notifyConsumer(){
        for(IConsumer consumer : consumerList.values()){
            consumer.update(this.name);
        }
    }

    public int getConsumerCount(){
        return consumerList.size();
    }

    public String getLastReadMessageId(IConsumer consumer){
        return consumerLastMessageId.get(consumer.getId());
    }

    public String setLastReadMessageId(IConsumer consumer, String messageId){
        return consumerLastMessageId.put(consumer.getId(), messageId);
    }
}
