package com.hooniegit.AeronClient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class AeronClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(AeronClientApplication.class, args);
    }

}
