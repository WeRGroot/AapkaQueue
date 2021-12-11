package com.nafka.repository;

import com.nafka.model.Topic;
import java.util.HashMap;
import java.util.Map;

public class TopicRepository {
    private Map<String, Topic> topicRepo;
    private static TopicRepository topicRepository;

    private TopicRepository(){
        topicRepo = new HashMap<>();
    }

    public static TopicRepository getInstance(){
        if(topicRepository == null){
            topicRepository = new TopicRepository();
        }
        return topicRepository;
    }

    public Topic createOrGet(String topicName){
        if(!topicRepo.containsKey(topicName)){
            topicRepo.put(topicName, new Topic(topicName));
        }
        return topicRepo.get(topicName);
    }

    public Topic removeTopic(String topicName){
        return topicRepo.remove(topicName);
    }
}
