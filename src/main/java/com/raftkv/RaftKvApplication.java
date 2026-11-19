package com.raftkv;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * Entry point. Each running process is ONE Raft node. Start five of them (see the README)
 * to get a cluster that tolerates two failures.
 *
 * <p>DataSource auto-configuration is disabled because PostgreSQL is optional. When
 * {@code raft.storage.type=postgres}, {@code RaftConfig} builds the connection pool itself.
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class RaftKvApplication {

    public static void main(String[] args) {
        SpringApplication.run(RaftKvApplication.class, args);
    }
}
