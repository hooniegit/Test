package com.hooniegit.AeronServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class AeronServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AeronServerApplication.class, args);
    }

}
