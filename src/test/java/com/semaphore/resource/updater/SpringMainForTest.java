package com.semaphore.resource.updater;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 *
 * @date 2022/1/25 2:13 PM
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.semaphore")
public class SpringMainForTest {
    public static void main(String[] args) {
        SpringApplication.run(SpringMainForTest.class, args);
    }
}
