/**
 * Copyright 2010-2016 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.flywaydb.core.api.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.qatools.embed.service.MongoEmbeddedService;


import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Test whether a readWriter user can baseline and migrate a DB that does not exist.
 */
public class MongoFlywayReadWriteUserTest  {

    private static final String DB = "flyway_test";
    private static final String USER = "rwuser";
    private static final String PWD = "rwpwd";
    private String mongoUri;
    private MongoClient mongoClient;
    private MongoEmbeddedService mongoEmbeddedService;


    @Before
    public void initialize() throws IOException {

        String replSetName = "localhost:" + 58675;//Network.getFreeServerPort();
        mongoUri = "mongodb://" + USER + ":" + PWD + "@" + replSetName + "/" + DB;

        // MongoEmbeddedService starts an embedded Mongo database with MONGODB-CR authentication mechanism.
        mongoEmbeddedService = new MongoEmbeddedService(
                replSetName,
                DB,
                USER,
                PWD,
                "local",
                null,
                true,
                30);
        // Starts embedded mongo and creates the user specified (USER) on the DB specified (DB) with readWrite role.
        mongoEmbeddedService.doStart();

        mongoClient = new MongoClient(new MongoClientURI(mongoUri));
    }


    @After
    public void tearDown() {
        mongoEmbeddedService.stop();
    }


    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public String getMongoUri() {
        return mongoUri;
    }

    public String getDatabaseName() {
        return DB;
    }

    private MongoFlyway build() {
        Properties props = new Properties();
        props.setProperty("flyway.locations", "migration.mongoscript");
        props.setProperty("flyway.validateOnMigrate", "false");
        props.setProperty("flyway.mongoUri", getMongoUri());

        MongoFlyway flyway = new MongoFlyway();
        flyway.configure(props);
        // Set mongo client so that flyway does not close it.
        flyway.setMongoClient(getMongoClient());
        return flyway;
    }

    private void createTestDb() {
        getMongoClient().getDatabase(getDatabaseName()).createCollection("demo");
    }

    @Test
    public void migrate() {
        MongoFlyway flyway = build();
        try {
            flyway.baseline();
            flyway.migrate();
        } catch (FlywayException e) {
            fail("Mongo baseline failed:" + e.getLocalizedMessage());
            return;
        }

        MigrationInfo current = flyway.info().current();
        assertEquals(MigrationVersion.fromVersion("2.0"), current.getVersion());
        assertEquals(MigrationType.MONGOSCRIPT, current.getType());
        assertEquals(MigrationState.SUCCESS, current.getState());
    }
}
