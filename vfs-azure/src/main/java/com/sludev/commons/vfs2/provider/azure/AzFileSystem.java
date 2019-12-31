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

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * File-System object represents a connect to Microsoft Azure Blob via a single client.
 *
 * @author Kervin Pierre
 */
public class AzFileSystem extends AbstractFileSystem implements FileSystem {

    private static final Logger log = LoggerFactory.getLogger(AzFileSystem.class);

    private final BlobContainerAsyncClient blobContainerAsyncClient;
    private final BlobContainerClient blobContainerClient;


    protected AzFileSystem(
            final AzFileName rootName,
            final BlobContainerAsyncClient blobContainerAsyncClient,
            final BlobContainerClient blobContainerClient,
            final FileSystemOptions fileSystemOptions) {

        super(rootName, null, fileSystemOptions);

        this.blobContainerAsyncClient = blobContainerAsyncClient;
        this.blobContainerClient = blobContainerClient;
    }

    protected BlobContainerAsyncClient getContainerAsyncClient()
    {
        return blobContainerAsyncClient;
    }
    protected BlobContainerClient getContainerClient()
    {
        return blobContainerClient;
    }

    @Override
    protected FileObject createFile(AbstractFileName name) throws Exception {
        return new AzFileObject(name, this);
    }


    @Override
    protected void addCapabilities(Collection<Capability> caps)
    {
        caps.addAll(AzFileProvider.capabilities);
    }

}
