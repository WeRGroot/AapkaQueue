package com.nafka.model;

import java.util.UUID;

public interface IConsumer {

    String getId();

    void update(String topicName);

}
