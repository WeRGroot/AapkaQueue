package com.nafka.model;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

public class NafkaQueue {
    private Map<String,  Map<String, Message>> messageQueue;
    private Map<String, Map<String, String>> nextMessageMap;
    private Map<String, String> currentMessageIdMap;
    private Map<String, String> topMessageIdMap;

    private static NafkaQueue nafkaQueue;

    private NafkaQueue(){
        this.messageQueue = new HashMap<>();
        this.nextMessageMap = new HashMap<>();
        this.currentMessageIdMap = new HashMap<>();
        this.topMessageIdMap = new HashMap<>();
    }

    public static NafkaQueue getInstance(){
        if(nafkaQueue == null){
            nafkaQueue = new NafkaQueue();
        }
        return nafkaQueue;
    }

    public boolean createTopic(String topicName){
        if(messageQueue.containsKey(topicName)){
            return false;
        }
        messageQueue.put(topicName, new HashMap<>());
        nextMessageMap.put(topicName, new HashMap<>());
        currentMessageIdMap.put(topicName, null);
        topMessageIdMap.put(topicName, null);
        return true;
    }

    public boolean deleteTopic(String topicName){
        return messageQueue.remove(topicName) != null && nextMessageMap.remove(topicName) != null;
    }

    public void addMessageToQueue(String topicName, Message message){
        if(!messageQueue.containsKey(topicName)){
            throw new RuntimeException("Invalid topic name");
        }
        String messageId =  message.getId();
        messageQueue.get(topicName).put(messageId, message);
        if(currentMessageIdMap.get(topicName) == null){
            topMessageIdMap.put(topicName, messageId);
        }else{
            nextMessageMap.get(topicName).put(currentMessageIdMap.get(topicName), messageId);
        }
        this.currentMessageIdMap.put(topicName,messageId);
    }

    public Message removeMessageFromQueue(String topicName){
        if(!messageQueue.containsKey(topicName)){
            throw new RuntimeException("Invalid topic name");
        }

        if(messageQueue.get(topicName).isEmpty()){
            return null;
        }
        String topMessageId = topMessageIdMap.get(topicName);
        String currentMessageId = currentMessageIdMap.get(topicName);

        Message message = messageQueue.get(topicName).remove(topMessageId);

        if(topMessageId == currentMessageId){
            topMessageIdMap.put(topicName, null);
            currentMessageIdMap.put(topicName, null);
        }else{
            String nextTopMessageId = nextMessageMap.get(topicName).get(topMessageId);
            nextMessageMap.get(topicName).remove(topMessageId);
            topMessageIdMap.put(topicName, nextTopMessageId);
        }
        return message;
    }

    public int getQueueSize(String topicName){
        return messageQueue.get(topicName).size();
    }

    public Message readMessage(String topicName, String lastReadMessageId){
        String nextMessageId = getNextMessageId(topicName, lastReadMessageId);
        Message message = messageQueue.get(topicName).get(nextMessageId);
        message.decrementAndGetCounterByOne();
        if(message.getCounter() == 0){
            removeMessageFromQueue(topicName);
        }
        return message;
    }

    private String getNextMessageId(String topicName, String lastMessageId){
        if(topMessageIdMap.get(topicName) == currentMessageIdMap.get(topicName)){
            return currentMessageIdMap.get(topicName);
        }

        return nextMessageMap.get(topicName).get(lastMessageId);
    }
}
