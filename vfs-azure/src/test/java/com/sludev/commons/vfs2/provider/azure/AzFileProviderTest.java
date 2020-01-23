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

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 *
 * @author kervin
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AzFileProviderTest
{
    private static final Logger log = LoggerFactory.getLogger(AzFileProviderTest.class);

    private Properties testProperties;
    private DefaultFileSystemManager fileSystemManager;
    private FileSystemOptions fileSystemOptions;
    private String azUri;

    private static File testFile;

    public AzFileProviderTest() {}

    @Rule
    public TestWatcher testWatcher = new AzTestWatcher();

    @Before
    public void setUp() {
        try {

            /**
             * Get the current test properties from a file so we don't hard-code
             * in our source code.
             */
            testProperties = AzTestProperties.GetProperties();

            String account = testProperties.getProperty("azure.account.name");
            String key = testProperties.getProperty("azure.account.key");
            String container = testProperties.getProperty("azure.test0001.container.name");
            String host = testProperties.getProperty("azure.host", account + ".blob.core.windows.net");

            fileSystemManager = new DefaultFileSystemManager();

            fileSystemManager.addProvider(AzConstants.AZBSSCHEME, new AzFileProvider());
            fileSystemManager.addProvider("file", new DefaultLocalFileProvider());
            fileSystemManager.init();

            fileSystemOptions = new FileSystemOptions();
            StaticUserAuthenticator auth = new StaticUserAuthenticator("", account, key);

            DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(fileSystemOptions, auth);

            azUri = String.format("%s://%s/%s/", AzConstants.AZBSSCHEME, host, container);
        }
        catch (Exception ex) {
            log.debug("Error setting up remote folder structure.  Have you set the test001.properties file?", ex);
        }
    }

    @After
    public void tearDown() throws Exception {
        fileSystemManager.close();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        testFile = createSmallFile();
    }

    @AfterClass
    public static void tearDownClass() {
        testFile.delete();
    }


    private static File createSmallFile() throws Exception {

        File file = File.createTempFile("uploadFile01", ".tmp");

//        RandomAccessFile raf = new RandomAccessFile(file, "rwd");
//        raf.setLength(1 * 1024 * 1024);

        try (FileWriter fw = new FileWriter(file)) {

            BufferedWriter bw = new BufferedWriter(fw);

            for (int i = 0; i < 100; i++) {
                bw.append("testing...");
                bw.flush();
            }
        }

        return file;
    }


    @Test
    public void testUploadFile() throws Exception {

        String fileName = "test01.tmp";
        String destUri = azUri + fileName;

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        assertTrue(destFileObject.exists());

        FileType fileType = destFileObject.getType();

        assertEquals(FileType.FILE, fileType);

        destFileObject.delete();
    }


    @Test
    public void testCopyBlobToBlob() throws Exception {

        String destUri = azUri + "test01.tmp";

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        // Move local to AZBS
        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        assertTrue(destFileObject.exists());

        // Setup and copy AZBS to AZBS
        String destUri2 = azUri + "test01-copyz.tmp";

        FileObject destCopyFileObject = fileSystemManager.resolveFile(destUri2, fileSystemOptions);

        destCopyFileObject.copyFrom(destFileObject, Selectors.SELECT_SELF);

        try {
            assertTrue(destCopyFileObject.exists());
        }
        finally {
            destFileObject.delete();
            destCopyFileObject.delete();
        }
    }


    @Test
    public void testDeleteFile() throws Exception {

        String fileName = "test01.tmp";
        String destUri = azUri + fileName;

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        assertTrue(destFileObject.exists());

        destFileObject.delete();

        assertFalse(destFileObject.exists());
    }


    @Test
    public void testGetType() throws Exception {

        String fileName = "test/test01.tmp";
        String destUri = azUri + fileName;

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        FileType fileType = destFileObject.getType();

        assertEquals(FileType.FILE, fileType);

        FileObject dirFileObject = fileSystemManager.resolveFile(azUri + "/test/", fileSystemOptions);
        fileType = dirFileObject.getType();

        assertEquals(FileType.FOLDER, fileType);

        FileObject imaginaryFileObject = fileSystemManager.resolveFile(azUri + "/test1/", fileSystemOptions);
        fileType = imaginaryFileObject.getType();

        assertEquals(FileType.IMAGINARY, fileType);

        FileObject rootFileObject2 = fileSystemManager.resolveFile(azUri + "/", fileSystemOptions);
        fileType = rootFileObject2.getType();

        assertEquals(FileType.FOLDER, fileType);

        destFileObject.delete();
    }


    @Test
    public void testCreateDirectory1() throws Exception {

        String fileName = "testDir01/test1.tmp";

        String destUri = azUri + fileName;

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        assertTrue(destFileObject.exists());

        destFileObject.delete();
    }


    @Test
    public void testDownloadFile() throws Exception {

        String destUri = azUri + "/test1.tmp";

        // Upload file

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        // Download file

        FileObject copyFileObject =
                fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath() + "-copy"));

        copyFileObject.copyFrom(destFileObject, Selectors.SELECT_SELF);

        assertTrue(copyFileObject.exists());

        copyFileObject.delete();
        destFileObject.delete();
    }


    @Test
    public void testExist() throws Exception {

        String destUri = azUri + "/test1.tmp";

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        assertTrue(destFileObject.exists());

        destFileObject.delete();

        destUri = azUri + "/non-existant-file.tmp";

        destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        assertFalse(destFileObject.exists());
    }


    @Test
    public void testGetContentSize() throws Exception {

        String destUri = azUri + "/test1.tmp";

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        FileContent fileContent = destFileObject.getContent();

        long size = fileContent.getSize();

        assertTrue(size > 0);

        destFileObject.delete();
    }


    /**
     * By default FileObject.getChildren() will use doListChildrenResolved() if available
     *
     * @throws Exception
     */
    @Test
    public void testListChildren() throws Exception {

        createFolderStructure();

        String destUri = azUri + "uploadFile02";

        FileObject fileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        FileObject[] children = fileObject.getChildren();

        assertTrue(children.length > 0);

        for (FileObject obj : children) {

            FileName currName = obj.getName();
            Boolean res = obj.exists();
            FileType ft = obj.getType();

            log.info( String.format("\nNAME.PATH : '%s'\nEXISTS : %b\nTYPE : %s\n\n", currName.getPath(), res, ft));
        }

        deleteFolderStructure();
    }


    @Test
    public void testContent() throws Exception {

        String destUri = azUri + "test01.tmp";

        FileObject srcFileObject = fileSystemManager.resolveFile(String.format("file://%s", testFile.getAbsolutePath()));
        FileObject destFileObject = fileSystemManager.resolveFile(destUri, fileSystemOptions);

        destFileObject.copyFrom(srcFileObject, Selectors.SELECT_SELF);

        FileContent content = destFileObject.getContent();

        long size = content.getSize();

        assertTrue( size > 0);

        long modTime = content.getLastModifiedTime();

        assertTrue(modTime > 0);

        destFileObject.delete();
    }


    public void createFolderStructure() throws Exception {

        String account = testProperties.getProperty("azure.account.name");
        String key = testProperties.getProperty("azure.account.key");
        String container = testProperties.getProperty("azure.test0001.container.name");
        String host = testProperties.getProperty("azure.host", account + ".blob.core.windows.net");


        File temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 01");

        AzTestUtils.uploadFile(account, host, key, container, temp.toPath(), Paths.get("file01.tmp"));

        temp.delete();

        temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 02");

        AzTestUtils.uploadFile(account, host, key, container, temp.toPath(), Paths.get("uploadFile02/file02.tmp"));

        temp.delete();

        temp = AzTestUtils.createTempFile("uploadFile02", "tmp", "File 03");

        AzTestUtils.uploadFile(account, host, key, container, temp.toPath(), Paths.get("uploadFile02/dir01/file03.tmp"));

        temp.delete();
    }


    public void deleteFolderStructure() throws Exception {

        String account = testProperties.getProperty("azure.account.name");
        String key = testProperties.getProperty("azure.account.key");
        String container = testProperties.getProperty("azure.test0001.container.name");
        String host = testProperties.getProperty("azure.host", account + ".blob.core.windows.net");

        AzTestUtils.deleteFile(account, host, key, container,
                               Paths.get("file01.tmp"));

        AzTestUtils.deleteFile(account, host, key, container,
                               Paths.get("uploadFile02/file02.tmp"));

        AzTestUtils.deleteFile(account, host, key, container,
                               Paths.get("uploadFile02/dir01/file03.tmp"));
    }
}
