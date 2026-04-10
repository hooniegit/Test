package com.hooniegit.AeronSubscriber;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class AeronSubscriberApplication {

    public static void main(String[] args) {
        SpringApplication.run(AeronSubscriberApplication.class, args);
    }

}
