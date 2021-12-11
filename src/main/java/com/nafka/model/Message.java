package com.nafka.model;

import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

@Getter
public class Message<T> {
    private String id;

    @Setter
    private T message;

    @Getter
    private int counter;

    public Message(T message, int counter) {
        this.id = UUID.randomUUID().toString();
        this.message = message;
        this.counter = counter;
    }

    public int decrementAndGetCounterByOne(){
        return --counter;
    }


}
