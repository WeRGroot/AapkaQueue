package com.nafka.service;

import com.nafka.model.IConsumer;
import com.nafka.model.Message;
import com.nafka.model.NafkaQueue;
import com.nafka.model.Topic;
import com.nafka.repository.TopicRepository;

public class TopicService {
    private NafkaQueue nafkaQueue;
    private TopicRepository topicRepository;
    private static TopicService topicService;

    private TopicService() {
        nafkaQueue = NafkaQueue.getInstance();
        topicRepository = TopicRepository.getInstance();
    }

    public static TopicService getInstance(){
        if(topicService == null){
            topicService = new TopicService();
        }
        return topicService;
    }

    public <T> void publish(String topicName, T messageBody){
        Topic topic = topicRepository.createOrGet(topicName);
        Message message = new Message(messageBody, topic.getConsumerCount());
        nafkaQueue.addMessageToQueue(topicName, message);
        topic.notifyConsumer();
    }

    public int getQueueSize(String topicName){
        return nafkaQueue.getQueueSize(topicName);
    }

    public Message readMessage(String topicName, IConsumer consumer){
        String lastMessageReadId = topicRepository.createOrGet(topicName).getLastReadMessageId(consumer);
        Message message = nafkaQueue.readMessage(topicName, lastMessageReadId);
        return message;
    }

    public void createTopic(String topicName){
        topicRepository.createOrGet(topicName);
        nafkaQueue.createTopic(topicName);
    }

    public void register(IConsumer consumer, String topicName){
        topicRepository.createOrGet(topicName).registerConsumer(consumer);
    }

    public void unregister(IConsumer consumer, String topicName){
        topicRepository.createOrGet(topicName).unregisterConsumer(consumer);
    }

}
