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
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.sas.SasProtocol;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;


/**
 * The main FileObject class in this provider.  It holds most of the API callbacks
 * for the provider.
 *
 * @author Kervin Pierre
 */
public class AzFileObject extends AbstractFileObject {

    private static final Logger log = LoggerFactory.getLogger(AzFileObject.class);

    public static final int MEGABYTES_TO_BYTES_MULTIPLIER = (int) Math.pow(2.0, 20.0);

    public static final int DEFAULT_UPLOAD_BLOCK_SIZE_MB = 4;
    public static final int BLOB_COPY_THRESHOLD_MB = 256 * 1024 * 1024;

    public static final long AZURE_MAX_BLOCKS = 50000L;
    public static final long AZURE_MAX_BLOCK_SIZE_MB = 100L;
    public static final long AZURE_MAX_BLOB_SIZE_BYTES = AZURE_MAX_BLOCK_SIZE_MB * MEGABYTES_TO_BYTES_MULTIPLIER * AZURE_MAX_BLOCKS;

    private static final float BLOCK_SIZE_SCALER =
            (float) AZURE_MAX_BLOCK_SIZE_MB / (float) (AZURE_MAX_BLOCK_SIZE_MB * AZURE_MAX_BLOCKS);

    private static Boolean ENABLE_AZURE_STORAGE_LOG = false;


    private FileType fileType = null;

    static {

        String uploadBlockSizeProperty = System.getProperty("azure.upload.block.size");

//        UPLOAD_BLOCK_SIZE_MB = (int) NumberUtils.toLong(uploadBlockSizeProperty, UPLOAD_BLOCK_SIZE_MB); //*
        // MEGABYTES_TO_BYTES_MULTIPLIER;

        String enableAzureLogging = System.getProperty("azure.enable.logging");

        if (StringUtils.isNotEmpty(enableAzureLogging)) {
            ENABLE_AZURE_STORAGE_LOG = BooleanUtils.toBoolean(enableAzureLogging);
        }

//        log.info("Azure upload block size : {} Bytes, concurrent request count: {}", UPLOAD_BLOCK_SIZE);
    }

//    private final AzFileSystem fileSystem;

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
    public AzFileObject(final AbstractFileName fileName, final AzFileSystem fileSystem) {

        super(fileName, fileSystem);

//        this.fileSystem = fileSystem;
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

        Iterable<BlobItem> blobs = blobContainerClient.listBlobsByHierarchy(name, lbo, null);

        List<BlobItem> blobList = new ArrayList<>();

        blobs.forEach(b -> {
            blobList.add(b);
        });

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
        else if (fileName.getType() == FileType.FOLDER) {
            // This is an empty folder.
            res = FileType.FOLDER;
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
     * @param overwrite true if the file should be overwritten.
     * @return
     * @throws Exception
     */
    @Override
    protected OutputStream doGetOutputStream(boolean overwrite) throws Exception {
        return blobClient.getBlockBlobClient().getBlobOutputStream(true);
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

        blobs.forEach(b -> {
            blobList.add(b);
        });

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

        if (FileType.FILE == doGetType()) {
            blobClient.delete();
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

        if (!blobClient.exists()) {
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
            FileType type = doGetType();
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
            if (doGetType().hasContent()) {
                throw new FileNotFolderException(getName());
            }
        }
        catch (Exception ex) {
            throw new FileNotFolderException(getName(), ex);
        }

        return super.getChildren();
    }


    @Override
    public void copyFrom(FileObject src, FileSelector selector)
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
                    if (srcFile.getType().hasChildren()) {
                        destFile.createFolder();
                    }
                    else if (canCopyServerSide(srcFile, destFile)) {

                        if (srcFile.getContent().getSize() > BLOB_COPY_THRESHOLD_MB) {
                            doCopyFromUrl((AzFileObject) srcFile);
                        }
                        else {
                            URL url = ((AzFileObject) srcFile).getSignedURL(24);
                            blobClient.copyFromUrl(url.toString());
                        }

                        doGetType(); // Change file to non-imgainary type.
                    }
                    else if (srcFile.getType().hasContent()) {

                        doCopyFromStream(srcFile, destFile);
                    }
                    else {
                        // nothing useful to do if no content and can't have children
                        throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile },
                                new UnsupportedOperationException());
                    }
                }
                catch (IOException | BlobStorageException e) {
                    throw new FileSystemException("vfs.provider/copy-file.error", new Object[] { srcFile, destFile }, e);
                }
            }
        }
        catch (Exception e) {
            throw new FileSystemException(e);
        }
    }


    /**
     * Copy a file object via input/output streams.
     *
     * @param srcFile
     * @param destFile
     * @throws IOException
     */
    private void doCopyFromStream(FileObject srcFile, FileObject destFile) throws Exception {

        try {
            String destFilename = destFile.getName().getPath();

            if (destFilename.startsWith("/")) {
                destFilename = destFilename.substring(1);
            }

            BlobClient destBlobClient = blobContainerClient.getBlobClient(destFilename);

            int blockSize = getBlockSize(srcFile.getContent().getSize());

            ParallelTransferOptions opts =
                    new ParallelTransferOptions(blockSize, 5, null);

            BlobRequestConditions requestConditions = new BlobRequestConditions();

            BlobOutputStream bos = destBlobClient.getBlockBlobClient().getBlobOutputStream(
                    opts, null, null, null, requestConditions);

            InputStream is = srcFile.getContent().getInputStream();

            try {
                byte[] buffer = new byte[4 * 1024 *1024];

                for (int len; (len = is.read(buffer)) != -1; ) {
                    bos.write(buffer, 0, len);
                }
            }
            finally {
                is.close();
                bos.close();
            }

            doGetType();
        }
        finally {
            destFile.close();
            srcFile.close();
        }
    }


    /**
     * Performas a blob to blob copy for files larger and 256MB.
     * @param srcFile
     * @throws Exception
     */
    private void doCopyFromUrl(AzFileObject srcFile) throws Exception {

        BlockBlobClient srcBlobClient = srcFile.blobClient.getBlockBlobClient();
        BlockBlobClient destBlobClient = blobClient.getBlockBlobClient();

        // Get the list of committed blocks
        BlockList blockList = srcBlobClient.listBlocks(BlockListType.COMMITTED);
        List<Block> blocks = blockList.getCommittedBlocks();

        long rangeMax = 0;
        URL blobUrl = srcFile.getSignedURL(24);

        List<String> blockIds = new ArrayList<>();

        // For each block copy the block using a signed URL.
        for (int j = 0; j < blocks.size(); j++) {

            Block block = blocks.get(j);
            long blockSize = block.getSize();
            BlobRange range = new BlobRange(rangeMax, blockSize);
            rangeMax += blockSize;

            blockIds.add(block.getName());
            destBlobClient.stageBlockFromUrl(block.getName(), blobUrl.toString(), range);
        }

        destBlobClient.commitBlockList(blockIds, true);
    }


    protected int getBlockSize(long fileSize) throws FileSystemException {

        if (fileSize > AZURE_MAX_BLOB_SIZE_BYTES) {
             throw new FileSystemException("File size exceeds Azure Blob size limit");
        }

        long dynamicBlockSizeThreshold = (DEFAULT_UPLOAD_BLOCK_SIZE_MB * AZURE_MAX_BLOCKS) * MEGABYTES_TO_BYTES_MULTIPLIER;

        if (fileSize < dynamicBlockSizeThreshold) {
            return DEFAULT_UPLOAD_BLOCK_SIZE_MB * MEGABYTES_TO_BYTES_MULTIPLIER;
        }
        else {
            return (int) Math.ceil(BLOCK_SIZE_SCALER * fileSize);
        }
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
     * @param durationHrs - SAS validity duration in hours
     * @return
     * @throws Exception
     */
    public URL getSignedURL(int durationHrs) throws Exception {

        doAttach();

        OffsetDateTime offsetDateTime = OffsetDateTime.now().plusHours(durationHrs);
        BlobSasPermission sasPermission = BlobSasPermission.parse("r");

        BlobServiceSasSignatureValues signatureValues = new BlobServiceSasSignatureValues(offsetDateTime, sasPermission);

        signatureValues.setStartTime(OffsetDateTime.now().minusMinutes(10));
        signatureValues.setProtocol(SasProtocol.HTTPS_ONLY);

        // Sign the url for the this object
        String url = this.blobClient.getBlobUrl() + "?" + blobClient.generateSas(signatureValues);

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

        return blobClient.getAccountName();
    }


    private BlobProperties getBlobProperties() throws Exception {

        if (blobProperties == null) {
            doAttach();
            blobProperties = blobClient.getProperties();
        }

        return blobProperties;
    }
}
