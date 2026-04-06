package com.hooniegit.AeronPublisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class AeronPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(AeronPublisherApplication.class, args);
    }

}
