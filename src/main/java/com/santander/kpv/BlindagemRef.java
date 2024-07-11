package com.santander.kpv;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@EnableJms
public class BlindagemRef {

    public static void main(String[] args) {

        SpringApplication.run(BlindagemRef.class, args);
    }

}
