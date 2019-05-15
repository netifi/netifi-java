package com.netifi.spring.boot;

import com.netifi.spring.core.config.BrokerClientConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@DirtiesContext
@ImportAutoConfiguration({
    BrokerClientMessagingAutoConfiguration.class,
    BrokerClientAutoConfiguration.class,
    BrokerClientConfiguration.class,
})
public class SpringMessagingIntegrationTest {

    @ClassRule
    public static GenericContainer redis =
        new GenericContainer("netifi/broker:1.6.2")
            .withExposedPorts(8001, 7001, 6001, 8101)
        .withEnv("BROKER_SERVER_OPTS", "-Dnetifi.broker.ssl.disabled=true " +
            "-Dnetifi.authentication.0.accessKey=9007199254740991 " +
            "-Dnetifi.authentication.0.accessToken=kTBDVtfRBO4tHOnZzSyY5ym2kfY= " +
            "-Dnetifi.broker.admin.accessKey=9007199254740991 " +
            "-Dnetifi.broker.admin.accessToken=kTBDVtfRBO4tHOnZzSyY5ym2kfY="
        );


    @Test
    public void tests() {

    }

}
