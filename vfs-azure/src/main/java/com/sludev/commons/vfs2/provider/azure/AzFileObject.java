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

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.common.Utility;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.net.io.CopyStreamListener;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * The main FileObject class in this provider.  It holds most of the API callbacks
 * for the provider.
 *
 * @author Kervin Pierre
 */
public class AzFileObject extends AbstractFileObject {

    private static final Logger log = LoggerFactory.getLogger(AzFileObject.class);
    private static final int MEGABYTES_TO_BYTES_MULTIPLIER = (int) Math.pow(2.0, 20.0);
    static Integer UPLOAD_BLOCK_SIZE = 20; //in MB's
    private static Boolean ENABLE_AZURE_STORAGE_LOG = false;
//    private static Tika tika = new Tika();

    private FileType fileType = null;

    static {

        String uploadBlockSizeProperty = System.getProperty("azure.upload.block.size");

        UPLOAD_BLOCK_SIZE = (int) NumberUtils.toLong(uploadBlockSizeProperty, UPLOAD_BLOCK_SIZE); //*
        // MEGABYTES_TO_BYTES_MULTIPLIER;

        String enableAzureLogging = System.getProperty("azure.enable.logging");

        if (StringUtils.isNotEmpty(enableAzureLogging)) {
            ENABLE_AZURE_STORAGE_LOG = BooleanUtils.toBoolean(enableAzureLogging);
        }

        log.info("Azure upload block size : {} Bytes, concurrent request count: {}", UPLOAD_BLOCK_SIZE);
    }

    private final AzFileSystem fileSystem;
    private BlobContainerClient blobContainerClient;
    private BlobContainerAsyncClient blobContainerAsyncClient;
    private BlobClient blobClient;
    private BlobAsyncClient blobAsyncClient;
    private BlobProperties blobProperties;
    private boolean isAttached = false;


    /**
     * Creates a new FileObject for use with a remote Azure Blob Storage file or folder.
     *
     * @param fileName
     * @param fileSystem
     */
    protected AzFileObject(final AbstractFileName fileName, final AzFileSystem fileSystem) {

        super(fileName, fileSystem);

        this.fileSystem = fileSystem;

        blobContainerAsyncClient = fileSystem.getContainerAsyncClient();
        blobContainerClient = fileSystem.getContainerClient();
        blobProperties = null;
    }


    public BlobAsyncClient getBlobAsyncClient() {

        return this.blobAsyncClient;
    }


    @Override
    protected void doAttach() throws Exception {

        if (isAttached) {
            return;
        }

        String name = getName().getPath();

        if (name.startsWith("/")) {
            name = name.substring(1);
        }

        BlobClient client = blobContainerClient.getBlobClient(name);
        BlobAsyncClient asyncClient = blobContainerAsyncClient.getBlobAsyncClient(name);

        if (asyncClient != null) {
            blobClient = client;
            blobAsyncClient = asyncClient;
            isAttached = true;
        }
    }


    /**
     * Callback for use when detaching this File Object from Azure Blob Storage.
     * <p>
     * The File Object should be reusable after <code>attach()</code> call.
     *
     * @throws Exception
     */
    @Override
    protected void doDetach() throws Exception {

        blobClient = null;
        blobAsyncClient = null;
        blobProperties = null;
        isAttached = false;
    }


    /**
     * Callback for checking the type of the current FileObject.  Typically can
     * be of type...
     * FILE for regular remote files
     * FOLDER for regular remote containers
     * IMAGINARY for a path that does not exist remotely.
     *
     * @return
     * @throws Exception
     */
    @Override
    protected FileType doGetType() throws Exception {

        doAttach();

        FileType res;

        AzFileName fileName = (AzFileName) getName();

        String name = fileName.getPath();

        if (name.startsWith("/")) {
            name = name.substring(1);
        }

        // If we are given the container root then consider this a folder.
        if ("".equals(name)) {
            return FileType.FOLDER;
        }

        ListBlobsOptions lbo = new ListBlobsOptions();

        lbo.setMaxResultsPerPage(2);
        lbo.setPrefix(name);

        Iterable<BlobItem> blobs = blobContainerAsyncClient.listBlobsByHierarchy(name, lbo).toIterable();

        List<BlobItem> blobList = new ArrayList<>();

        // Pull it all in memory and work from there
        CollectionUtils.addAll(blobList, blobs);


        if (blobList.size() > 1) {
            res = FileType.FOLDER;
        }
        else if (blobList.size() == 1) {

            BlobItem item = blobList.get(0);

            if (item.isPrefix() != null && item.isPrefix()) {
                res = FileType.FOLDER;
            }
            else {
                res = FileType.FILE;
            }
        }
        else {
            res = FileType.IMAGINARY;
        }

        fileType = res;

        return fileType;
    }


    //    @Override
    //    protected FileObject[] doListChildrenResolved() throws Exception
    //    {
    //        FileObject[] res = null;
    //
    //        Pair<String, String> path = getContainerAndPath();
    //
    //        String prefix = path.getRight();
    //        if( prefix.endsWith("/") == false )
    //        {
    //            // We need folders ( prefixes ) to end with a slash
    //            prefix += "/";
    //        }
    //
    //        Iterable<ListBlobItem> blobs = null;
    //        if( prefix.equals("/") )
    //        {
    //            // Special root path case. List the root blobs with no prefix
    //            blobs = currContainer.listBlobs();
    //        }
    //        else
    //        {
    //            blobs = currContainer.listBlobs(prefix);
    //        }
    //
    //        List<ListBlobItem> blobList = new ArrayList<>();
    //
    //        // Pull it all in memory and work from there
    //        CollectionUtils.addAll(blobList, blobs);
    //        ArrayList<AzFileObject> resList = new ArrayList<>();
    //        for(ListBlobItem currBlobItem : blobList )
    //        {
    //            String currBlobStr = currBlobItem.getUri().getPath();
    //            AzFileObject childBlob = new AzFileObject();
    //            FileName currName = getFileSystem().getFileSystemManager().resolveName(name, file, NameScope.CHILD);
    //
    //            resList.add(currBlobStr);
    //        }
    //
    //        res = resList.toArray(new String[resList.size()]);
    //
    //        return res;
    //    }


    /**
     * Callback for handling "content size" requests by the provider.
     *
     * @return The number of bytes in the File Object's content
     * @throws Exception
     */
    @Override
    protected long doGetContentSize() throws Exception {

        long res = -1;

        getBlobProperties();
        res = getBlobProperties().getBlobSize();

        return res;
    }


    /**
     * Get an InputStream for reading the content of this File Object.
     *
     * @return The InputStream object for reading.
     * @throws Exception
     */
    @Override
    protected InputStream doGetInputStream() throws Exception {
        return blobClient.getBlockBlobClient().openInputStream();
    }


    /**
     * Callback for getting an OutputStream for writing into Azure Blob Storage file.
     *
     * @param bAppend bAppend true if the file should be appended to, false if it should be overwritten.
     * @return
     * @throws Exception
     */
    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
        return blobClient.getBlockBlobClient().getBlobOutputStream();
    }


    /**
     * Lists the children of this file.  Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}.  The return value of this method
     * is cached, so the implementation can be expensive.<br />
     *
     * @return a possible empty String array if the file is a directory or null or an exception if the
     * file is not a directory or can't be read.
     * @throws Exception if an error occurs.
     */
    @Override
    protected String[] doListChildren() throws Exception {

        AzFileName fileName = (AzFileName) getName();

        String path = fileName.getPath();

        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        if (!path.endsWith("/")) {
            path = path + "/";
        }

        Iterable<BlobItem> blobs = blobContainerAsyncClient.listBlobsByHierarchy(path).toIterable();

        List<BlobItem> blobList = new ArrayList<>();

        // Pull it all in memory and work from there
        CollectionUtils.addAll(blobList, blobs);

        ArrayList<String> resList = new ArrayList<>();


        for (BlobItem blobItem : blobList) {

            String name = blobItem.getName();
            String[] names = name.split("/");

            String itemName = names[names.length - 1];

            // Preserve folders
            if (name.endsWith("/")) {
                itemName = itemName + "/";
            }

            resList.add(itemName);
        }

        String[] res = resList.toArray(new String[resList.size()]);

        return res;
    }


    /**
     * Callback for handling create folder requests.  Since there are no folders
     * in Azure Cloud Storage this call is ignored.
     *
     * @throws Exception
     */
    @Override
    protected void doCreateFolder() throws Exception {

        log.info(String.format("doCreateFolder() called."));
    }


    /**
     * Callback for handling delete on this File Object
     *
     * @throws Exception
     */
    @Override
    protected void doDelete() throws Exception {

        if (FileType.FILE == getType()) {
            blobAsyncClient.delete().block();
        }
    }


    /**
     * Callback for handling the <code>getLastModifiedTime()</code> Commons VFS API call.
     *
     * @return Time since the file has last been modified
     * @throws Exception
     */
    @Override
    protected long doGetLastModifiedTime() throws Exception {

        if (!blobAsyncClient.exists().block()) {
            return 0;
        }

        return getBlobProperties().getLastModified().toEpochSecond();
    }


    /**
     * We need to override this method, because the parent one throws an exception.
     *
     * @param modtime the last modified time to set.
     * @return true if setting the last modified time was successful.
     * @throws Exception
     */
    @Override
    protected boolean doSetLastModifiedTime(long modtime) throws Exception {

        return true;
    }


    /**
     * Determines if the file exists.
     *
     * @return true if the file exists, false otherwise,
     * @throws FileSystemException if an error occurs.
     */
    @Override public boolean exists() throws FileSystemException {

        try {
            FileType type = getType();
            return FileType.IMAGINARY != type;
        }
        catch (Exception e) {
            throw new FileSystemException(e);
        }
    }


    /**
     * This will prepare the fileObject to get resynchronized with the underlying file system if required.
     *
     * @throws FileSystemException if an error occurs.
     */
    @Override public void refresh() throws FileSystemException {
        // Noop
    }


    /**
     * Returns the list of children.
     *
     * @return The list of children
     * @throws FileSystemException If there was a problem listing children
     * @see AbstractFileObject#getChildren()
     */
    @Override
    public FileObject[] getChildren() throws FileSystemException {

        try {
            // Folders which are copied from other folders, have type = IMAGINARY. We can not throw exception based on folder
            // type only and so we have check here for content.
            if (getType().hasContent()) {
                throw new FileNotFolderException(getName());
            }
        }
        catch (Exception ex) {
            throw new FileNotFolderException(getName(), ex);
        }

        return super.getChildren();
    }


    /**
     * Override to use Azure Blob Java Client library in upload. This is more efficient then using default.
     */
    @Override
    public void copyFrom(final FileObject file, final FileSelector selector)
            throws FileSystemException {

        this.copyFrom(file, selector, null);
    }


    public void copyFrom(FileObject src, FileSelector selector, CopyStreamListener copyStreamListener)
            throws FileSystemException {

        if (!src.exists()) {
            throw new FileSystemException("vfs.provider/copy-missing-file.error", src);
        }

        try {

            doAttach();
            ArrayList files = new ArrayList();
            src.findFiles(selector, false, files);

            for (int i = 0; i < files.size(); ++i) {

                FileObject srcFile = (FileObject) files.get(i);
                FileType srcFileType = srcFile.getType();

                if (FileType.FOLDER == srcFileType) {
                    continue;
                }

                String relPath = src.getName().getRelativeName(srcFile.getName());
                FileObject destFile = this.resolveFile(relPath, NameScope.DESCENDENT_OR_SELF);

                if (destFile.exists() && destFile.getType() != srcFile.getType()) {
                    destFile.delete(Selectors.SELECT_ALL);
                }

                // We need the CloudBlockBlob for the file that we want to upload, as we were always using the
                // CloudBlockBlob of the root directory when we were trying to copy directories, hence it was always overwriting
                // the root directory on azure storage.
                //                CloudBlockBlob fileCurrBlob = getFileCurrBlob(destFile);

                try {
//                    if (srcFile.getType().hasChildren()) {
//                        destFile.createFolder();
//                    }
//                    else
                    if (canCopyServerSide(srcFile, destFile)) {

                        // Azure to Azure copy
                        //
                        //                        CloudBlockBlob currDestinationBlob = ((AzFileObject) destFile).getBlobClient();
                        //                        CloudBlockBlob currSourceBlob = ((AzFileObject) srcFile).getBlobClient();
                        //
                        //                        try {
                        //                            currDestinationBlob.startCopy(currSourceBlob);
                        //                        }
                        //                        catch (URISyntaxException e) {
                        //                            throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile }, e);
                        //                        }
                        //                        finally {
                        //                            destFile.close();
                        //                            srcFile.close();
                        //                        }
                        URL url = ((AzFileObject) src).getSignedURL(24);

                        String srcUrl = url.toString();

                        blobClient.getBlockBlobClient().copyFromUrl(srcUrl);
                    }
                    else if (srcFile.getType().hasContent()) {

                        try {

                            String destFilename = destFile.getName().getPath();

                            if (destFilename.startsWith("/")) {
                                destFilename = destFilename.substring(1);
                            }

                            BlobAsyncClient client = blobContainerAsyncClient.getBlobAsyncClient(destFilename);

                            int blockSize = getBlockSize(srcFile.getContent().getSize(), UPLOAD_BLOCK_SIZE);

                            ParallelTransferOptions opts = new ParallelTransferOptions(blockSize, 4, null);

                            InputStream is = srcFile.getContent().getInputStream();
                            long srcSize = srcFile.getContent().getSize();

                            Flux<ByteBuffer> fbb = Utility.convertStreamToByteBuffer(is, srcSize, blockSize);

                            client.upload(fbb, opts, true).block();;
                        }
                        finally {
                            destFile.close();
                            srcFile.close();
                        }
                    }
                    else {
                        // nothing useful to do if no content and can't have children
                        throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile },
                                new UnsupportedOperationException());
                    }
                }
                catch (IOException io) {
                    throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile }, io);
                }
                catch (BlobStorageException se) {
                    throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile }, se);
                }
            }
        }
        catch (Exception e) {
            throw new FileSystemException(e);
        }
    }


    private int getBlockSize(long fileSize, int maxBlockSize) {

        int blockSize = maxBlockSize * MEGABYTES_TO_BYTES_MULTIPLIER;

        long sizePerThread = (long) Math.floor(fileSize / 4.0);

        if (sizePerThread < blockSize) {
            blockSize = (int) (sizePerThread / 4);
        }

        blockSize = blockSize == 0 ? 4 : blockSize;

        return blockSize;
    }


    /**
     * Compares credential to check possibilities of copying file at server side.
     *
     * @param sourceFileObject
     * @param destinationFileObject
     * @return
     */
    private boolean canCopyServerSide(FileObject sourceFileObject, FileObject destinationFileObject) {

        if (!(sourceFileObject instanceof AzFileObject) || !(destinationFileObject instanceof AzFileObject)) {
            return false;
        }

        AzFileObject azSourceFileObject = (AzFileObject) sourceFileObject;
        AzFileObject azDestinationFileObject = (AzFileObject) destinationFileObject;

        String sourceAccountName = getAccountName(azSourceFileObject);
        String destinationAccountName = getAccountName(azDestinationFileObject);

        return sourceAccountName != null
                && destinationAccountName != null
                && sourceAccountName.equals(destinationAccountName);
    }


    /**
     * Returns false to reply on copyFrom method in case moving/copying file within same azure container
     *
     * @param fileObject
     * @return
     */
    @Override
    public boolean canRenameTo(FileObject fileObject) {

        return false;
    }


    /**
     * Generate signed url to directly access file.
     *
     * @param duration - in hours
     * @return
     * @throws Exception
     */
    public URL getSignedURL(int durationHrs) throws Exception {

        Date expiry = new Date();
        expiry.setTime(expiry.getTime() + (durationHrs * 60 * 60));

//        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
//
//        policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ));
//        policy.setSharedAccessStartTime(now);
//        policy.setSharedAccessExpiryTime(calendar.getTime());

        String url = this.blobClient.getBlobUrl();

        return new URL(url);
    }

    /**
     * Returns an account name from given azure file object
     *
     * @param azFileObject
     * @return
     */
    private String getAccountName(AzFileObject azFileObject) {

        AzFileSystem azFileSystem = (AzFileSystem) azFileObject.getFileSystem();

        return blobAsyncClient.getAccountName();
    }


    private BlobProperties getBlobProperties() throws Exception {

        if (blobProperties == null) {
            doAttach();
            blobProperties = blobAsyncClient.getProperties().block();
        }

        return blobProperties;
    }
}
