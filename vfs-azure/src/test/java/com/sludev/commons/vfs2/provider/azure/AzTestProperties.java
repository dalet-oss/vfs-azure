/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sludev.commons.vfs2.provider.azure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 *
 * @author kervin
 */
public class AzTestProperties
{
    private static final Logger log = LoggerFactory.getLogger(AzTestProperties.class);

    private static final Set<String> OVERRIDABLE_PROPERTIES = new HashSet<String>() {{
       add("azure.account.name");
       add("azure.account.key");
       add("azure.test0001.container.name");
    }};

    public static Properties GetProperties() {

        Properties testProperties = new Properties();

        try {
            testProperties.load(AzTestProperties.class
                            .getClassLoader()
                            .getResourceAsStream("test001.properties"));
        }
        catch (IOException ex) {
            log.error("Error loading properties file", ex);
        }

        // Allow override with environment variables.
        for (String propertyKey : OVERRIDABLE_PROPERTIES) {

            String envPropertyKey = propertyKey.toUpperCase().replace('.', '_');
            String envPropertyValue = System.getenv(envPropertyKey);

            if (envPropertyValue != null) {
                log.info("Overriding {} property with value from environment variable {}", propertyKey, envPropertyKey);
                testProperties.setProperty(propertyKey, envPropertyValue);
            }
        }

        return testProperties;
    }
}
