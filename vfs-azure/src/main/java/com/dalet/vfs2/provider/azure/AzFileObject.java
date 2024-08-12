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
package com.dalet.vfs2.provider.azure;

import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.common.sas.SasProtocol;
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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;


/**
 * The main FileObject class in this provider.  It holds most of the API callbacks
 * for the provider.
 */
public class AzFileObject extends AbstractFileObject<AzFileSystem> {

    private static final Logger log = LoggerFactory.getLogger(AzFileObject.class);

    protected static final long MEGABYTES_TO_BYTES_MULTIPLIER = (int) Math.pow(2.0, 20.0);

    // Increased default size : Fix for FLEX-64152
    protected static final int DEFAULT_UPLOAD_BLOCK_SIZE_MB = 8;
    private static final int TWENTY_FOUR_HOURS_IN_SEC = 24 * 60 * 60;

    private static final long STREAM_BUFFER_SIZE_MB = DEFAULT_UPLOAD_BLOCK_SIZE_MB * MEGABYTES_TO_BYTES_MULTIPLIER;
    private static final long BLOB_COPY_THRESHOLD_MB = 256 * MEGABYTES_TO_BYTES_MULTIPLIER;

    private static final int AZURE_MAX_BLOCKS = 50000;
    private static final int AZURE_MAX_BLOCK_SIZE_MB = 100;
    private static final long AZURE_MAX_BLOB_SIZE_BYTES =
            AZURE_MAX_BLOCK_SIZE_MB * MEGABYTES_TO_BYTES_MULTIPLIER * AZURE_MAX_BLOCKS;

    private static final String SLASH = "/";

    private final BlobContainerClient blobContainerClient;
    private final BlobContainerAsyncClient blobContainerAsyncClient;
    private BlobClient blobClient;
    private BlobProperties blobProperties;

    private FileType fileType = null;
    private boolean isAttached = false;


    /**
     * Creates a new FileObject for use with a remote Azure Blob Storage file or folder.
     *
     * @param fileName   - Azure file name object, which contains path and name of the file
     * @param fileSystem - Azure file system object for file operation
     */
    public AzFileObject(final AbstractFileName fileName, final AzFileSystem fileSystem) {

        super(fileName, fileSystem);

        blobContainerAsyncClient = fileSystem.getContainerAsyncClient();
        blobContainerClient = fileSystem.getContainerClient();
        blobProperties = null;
    }


    @Override
    protected void doAttach() {

        if (isAttached) {
            return;
        }

        String name = getName().getPath();

        if (name.startsWith(SLASH)) {
            name = name.substring(1);
        }

        BlobClient client = blobContainerClient.getBlobClient(name);

        if (client != null) {
            blobClient = client;
            isAttached = true;
        }
    }


    /**
     * Callback for use when detaching this File Object from Azure Blob Storage.
     * <p>
     * The File Object should be reusable after <code>attach()</code> call.
     */
    @Override
    protected void doDetach() {

        blobClient = null;
        blobProperties = null;
        isAttached = false;
        fileType = null;
    }


    /**
     * Callback for checking the type of the current FileObject.  Typically can
     * be of type...
     * FILE for regular remote files
     * FOLDER for regular remote containers
     * IMAGINARY for a path that does not exist remotely.
     *
     * @return - File Type of current azure url
     */
    @Override
    protected FileType doGetType() {

        doAttach();

        AzFileName fileName = (AzFileName) getName();

        //file type IMAGINARY check is required because in case of place holder type object file type would be IMAGINARY so
        // that needs to be corrected once it gets imported.
        // second reason behind this check is, this.isAttached and super.attached properties are not in sync when this
        // .doAttached() called directly so while closing object (fleObject.close()) internally it calls detach() method to
        // detach the object but it finds supper.attached to false and return from there without detaching the object.
        if (this.fileType != null && this.fileType != FileType.IMAGINARY) {
            return this.fileType;
        }

        if (fileName != null && fileName.getType() == FileType.FOLDER) {
            this.fileType = FileType.FOLDER;
            injectType(this.fileType);
            return this.fileType;
        }

        String name = fileName.getPath();

        if (name.startsWith(SLASH)) {
            name = name.substring(1);
        }

        // If we are given the container root then consider this a folder.
        if ("".equals(name)) {
            this.fileType = FileType.FOLDER;
            injectType(this.fileType);
            return this.fileType;
        }

        Iterable<BlobItem> blobs = blobContainerClient.listBlobsByHierarchy(name);
        BlobItem blobItem = null;

        Iterator<BlobItem> iterator = blobs.iterator();

        while (iterator.hasNext()) {

            BlobItem item = iterator.next();

            if (item.getName().equals(name) || item.getName().equals(name + SLASH)) {
                blobItem = item;
                break;
            }
        }

        FileType res;
        if (blobItem == null) {
            res = FileType.IMAGINARY;
        }
        else if (blobItem.isPrefix() != null && blobItem.isPrefix()) {
            res = FileType.FOLDER;
        }
        else {
            res = FileType.FILE;
        }

        this.fileType = res;
        super.injectType(this.fileType);

        return this.fileType;
    }


    /**
     * Callback for handling "content size" requests by the provider.
     *
     * @return The number of bytes in the File Object's content
     */
    @Override
    protected long doGetContentSize() {

        return getBlobProperties().getBlobSize();
    }


    /**
     * Get an InputStream for reading the content of this File Object.
     *
     * @return The InputStream object for reading.
     */
    @Override
    protected InputStream doGetInputStream() {

        return blobClient.getBlockBlobClient().openInputStream();
    }


    /**
     * Callback for getting an OutputStream for writing into Azure Blob Storage file.
     *
     * @param overwrite true if the file should be overwritten.
     * @return - output stream of current blob
     */
    @Override
    protected OutputStream doGetOutputStream(boolean overwrite) {

        return blobClient.getBlockBlobClient().getBlobOutputStream(true);
    }


    /**
     * Lists the children of this file.  Is only called if {@link #doGetType}
     * returns {@link FileType#FOLDER}.  The return value of this method
     * is cached, so the implementation can be expensive.<br />
     *
     * @return a possible empty String array if the file is a directory or null or an exception if the
     * file is not a directory or can't be read.
     */
    @Override
    protected String[] doListChildren() {

        AzFileName fileName = (AzFileName) getName();

        String path = fileName.getPath();

        if (path.equals(SLASH)) {
            path = ""; //root path
        }
        else {
            if (path.startsWith(SLASH)) {
                path = path.substring(1);
            }

            if (!path.endsWith(SLASH)) {
                path = path + SLASH;
            }
        }

        Iterable<BlobItem> blobs = blobContainerAsyncClient.listBlobsByHierarchy(path).toIterable();

        List<BlobItem> blobList = new ArrayList<>();

        blobs.forEach(blobList::add);

        ArrayList<String> resList = new ArrayList<>();

        for (BlobItem blobItem : blobList) {

            String name = blobItem.getName();
            String[] names = name.split(SLASH);

            String itemName = names[names.length - 1];

            // Preserve folders
            if (name.endsWith(SLASH)) {
                itemName = itemName + SLASH;
            }

            resList.add(itemName);
        }

        return resList.toArray(new String[resList.size()]);
    }


    /**
     * Callback for handling create folder requests.  Since there are no folders
     * in Azure Cloud Storage this call is ignored.
     */
    @Override
    protected void doCreateFolder() {

        log.debug("doCreateFolder() called.");
    }


    /**
     * Callback for handling delete on this File Object
     */
    @Override
    protected void doDelete() {

        if (FileType.FILE == doGetType()) {
            blobClient.delete();
        }

        //once object gets deleted fileType must be set as FileType.IMAGINARY because it's no longer exist.
        this.fileType = FileType.IMAGINARY;
    }


    /**
     * Callback for handling the <code>getLastModifiedTime()</code> Commons VFS API call.
     *
     * @return Time since the file has last been modified
     */
    @Override
    protected long doGetLastModifiedTime() {

        if (Boolean.FALSE.equals(blobClient.exists())) {
            return 0;
        }

        return getBlobProperties().getLastModified().toInstant().toEpochMilli();
    }


    /**
     * We need to override this method, because the parent one throws an exception.
     *
     * @param modifiedTime the last modified time to set.
     * @return true if setting the last modified time was successful.
     */
    @Override
    protected boolean doSetLastModifiedTime(long modifiedTime) {

        return true;
    }


    /**
     * Determines if the file exists.
     *
     * @return true if the file exists, false otherwise,
     * @throws FileSystemException if an error occurs.
     */
    @Override
    public boolean exists() throws FileSystemException {

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
     */
    @Override
    public void refresh() {
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

            List<FileObject> files = new ArrayList<>();
            src.findFiles(selector, false, files);

            for (FileObject srcFile : files) {

                FileType srcFileType = srcFile.getType();

                if (FileType.FOLDER == srcFileType) {
                    continue;
                }

                String relPath = src.getName().getRelativeName(srcFile.getName());
                FileObject destFile = this.resolveFile(relPath, NameScope.DESCENDENT_OR_SELF);

                if (destFile.exists() && destFile.getType() != srcFile.getType()) {
                    destFile.delete(Selectors.SELECT_ALL);
                }

                try {
                    if (srcFile.getType().hasChildren()) {
                        destFile.createFolder();
                    }
                    else if (canCopyServerSide(srcFile, destFile)) {

                        AzFileObject destAzFile = (AzFileObject) destFile;

                        String url = ((AzFileObject) srcFile).getSignedUrl(TWENTY_FOUR_HOURS_IN_SEC).toString();

                        if (srcFile.getContent().getSize() > BLOB_COPY_THRESHOLD_MB) {
                            SyncPoller<BlobCopyInfo, Void> poll = destAzFile.blobClient.beginCopy(url, Duration.ofSeconds(1));
                            PollResponse<BlobCopyInfo> pollResponse = poll.waitForCompletion();

                            if (pollResponse.getStatus() != LongRunningOperationStatus.SUCCESSFULLY_COMPLETED) {
                                Exception exception = new Exception(pollResponse.getStatus().toString());
                                throw new FileSystemException("vfs.provider/copy-file.error", exception, srcFile, destFile);
                            }
                        }
                        else {
                            destAzFile.blobClient.copyFromUrl(url);
                        }

                        destAzFile.doGetType(); // Change file to non-imaginary type.
                    }
                    else if (srcFile.getType().hasContent()) {

                        doCopyFromStream(srcFile, destFile);
                    }
                    else {
                        // nothing useful to do if no content and can't have children
                        throw new FileSystemException("vfs.provider/copy-file.error",
                                new UnsupportedOperationException(), srcFile, destFile);
                    }
                }
                catch (IOException | BlobStorageException e) {
                    throw new FileSystemException("vfs.provider/copy-file.error", e, srcFile, destFile);
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
     * @param srcFile  - source file to copy
     * @param destFile - destination file into which copy
     * @throws Exception - throw exception in case unexpected situation occurs
     */
    private void doCopyFromStream(FileObject srcFile, FileObject destFile) throws Exception {

        try {
            String destFilename = destFile.getName().getPath();

            if (destFilename.startsWith(SLASH)) {
                destFilename = destFilename.substring(1);
            }

            BlobClient destBlobClient = blobContainerClient.getBlobClient(destFilename);

            long blockSize = getBlockSize(srcFile.getContent().getSize());

            ParallelTransferOptions opts = new ParallelTransferOptions()
                    .setBlockSizeLong(blockSize)
                    .setMaxConcurrency(5);

            BlobRequestConditions requestConditions = new BlobRequestConditions();

            try (BlobOutputStream bos = destBlobClient.getBlockBlobClient().getBlobOutputStream(
                    opts, null, null, null, requestConditions);

                    InputStream is = srcFile.getContent().getInputStream()) {

                byte[] buffer = new byte[(int) STREAM_BUFFER_SIZE_MB];

                for (int len; (len = is.read(buffer)) != -1; ) {
                    bos.write(buffer, 0, len);
                }
            }

            ((AzFileObject) destFile).doGetType();
        }
        finally {
            destFile.close();
            srcFile.close();
        }
    }


    /**
     * Returns the block size depending on the size of the file to be uploaded.
     * A default block size of 4MB is used until the file size is larger than
     * 4mb * 50000, after this block size is scaled so that 50000 blocks are used.
     *
     * @param fileSize - file size for which block size to be decided
     * @return block size based on given file size
     * @throws FileSystemException - will be thrown in case of unexpected situation occur.
     */
    protected long getBlockSize(long fileSize) throws FileSystemException {

        if (fileSize > AZURE_MAX_BLOB_SIZE_BYTES) {
            throw new FileSystemException("File size exceeds Azure Blob size limit");
        }

        long dynamicBlockSizeThreshold = (DEFAULT_UPLOAD_BLOCK_SIZE_MB * AZURE_MAX_BLOCKS) * MEGABYTES_TO_BYTES_MULTIPLIER;

        if (fileSize < dynamicBlockSizeThreshold) {
            return DEFAULT_UPLOAD_BLOCK_SIZE_MB * MEGABYTES_TO_BYTES_MULTIPLIER;
        }
        else {
            return (long) Math.ceil((float) fileSize / (float) AZURE_MAX_BLOCKS);
        }
    }


    /**
     * Compares credential to check possibilities of copying file at server side.
     *
     * @param sourceFileObject      - source file object to copy
     * @param destinationFileObject - destination file object into which copy
     * @return - boolean flag to decide server side copy possible or not
     */
    private boolean canCopyServerSide(FileObject sourceFileObject, FileObject destinationFileObject) {

        if (!(sourceFileObject instanceof AzFileObject) || !(destinationFileObject instanceof AzFileObject)) {
            return false;
        }

        AzFileObject azSourceFileObject = (AzFileObject) sourceFileObject;
        AzFileObject azDestinationFileObject = (AzFileObject) destinationFileObject;

        String sourceAccountName = getAccountName(azSourceFileObject);
        String destinationAccountName = getAccountName(azDestinationFileObject);

        return sourceAccountName != null && sourceAccountName.equals(destinationAccountName);
    }


    /**
     * Returns false to reply on copyFrom method in case moving/copying file within same azure container
     *
     * @param fileObject - file object for which renamed can be decided
     * @return - always return false, renamed cannot be done
     */
    @Override
    public boolean canRenameTo(FileObject fileObject) {

        return false;
    }


    /**
     * Generate signed url to directly access file.
     *
     * @param durationSec - SAS validity duration in hours
     * @return Signed URL to process
     * @throws Exception - will be thrown in case of unexpected situation occur.
     */
    public URL getSignedUrl(int durationSec) throws Exception {

        doAttach();

        OffsetDateTime offsetDateTime = OffsetDateTime.now().plusSeconds(durationSec);
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
     * @param azFileObject - Azure file object for which account name to be returned
     * @return - name of account for given azure file object
     */
    private String getAccountName(AzFileObject azFileObject) {

        AzFileSystem azFileSystem = (AzFileSystem) azFileObject.getFileSystem();
        return azFileSystem.getContainerClient().getAccountName();
    }


    private BlobProperties getBlobProperties() {

        if (blobProperties == null) {
            doAttach();
            blobProperties = blobClient.getProperties();
        }

        return blobProperties;
    }

}
