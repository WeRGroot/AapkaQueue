package com.nafka;

import com.nafka.client.Consumer;
import com.nafka.client.Producer;
import com.nafka.service.TopicService;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        TopicService topicService = TopicService.getInstance();
        Producer slb = new Producer("Sanjay LB");
        Producer karanJohar = new Producer("Karan Johar");

        Consumer pvr = new Consumer("PVR");
        Consumer inox = new Consumer("INOX");
        Consumer imax = new Consumer("I-MAX");

        String movies = "movies";
        String webSeries = "Web Series";

        topicService.createTopic(movies);
        topicService.createTopic(webSeries);

        topicService.register(pvr, movies);
        topicService.register(inox, movies);
        topicService.register(imax, movies);


        topicService.register(pvr, webSeries);
        topicService.register(inox, webSeries);

        slb.publish(movies, "Padmavat");
        slb.publish(movies, "Ram Leela");

        topicService.unregister(pvr, movies);

        karanJohar.publish(movies, "My name is Khan");
        slb.publish(webSeries, "Saraswatichandra");
        slb.publish(movies, 7);
        slb.publish(movies, 2.0);

    }
}
