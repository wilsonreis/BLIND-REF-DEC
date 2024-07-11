package com.santander.kpv.utils;


import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

@Component
public class FileLoader implements ApplicationRunner {

    StringBuilder arquivoLido = new StringBuilder();

    String arquivo;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        ClassPathResource resource = new ClassPathResource("file001.txt");
        try (InputStream inputStream = resource.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                arquivoLido.append(line);
            }
            arquivo = arquivoLido.toString();
            arquivoLido = null;
        }
    }

    @Bean(name = "getXmlFile")
    public String getXmlFile() {
        return arquivo;
    }
}
