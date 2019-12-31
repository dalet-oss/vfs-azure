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
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 *
 * @author kervin
 */
public class AzTestWatcher extends TestWatcher {
    private static final Logger log = LoggerFactory.getLogger(AzTestWatcher.class);

    @Override
    protected void failed(Throwable e, Description description) {
        log.info(String.format("%s failed %s", description.getDisplayName(), e.getMessage()));
        super.failed(e, description);
    }

    @Override
    protected void succeeded(Description description) {
        log.info(String.format("%s succeeded.", description.getDisplayName()));
        super.succeeded(description);
    }
}
