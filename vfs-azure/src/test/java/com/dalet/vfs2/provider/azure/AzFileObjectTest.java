package com.dalet.vfs2.provider.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobProperties;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class AzFileObjectTest {

    private final static AzFileSystem fileSystem = mock(AzFileSystem.class);

    private void setup(BlobContainerClient containerClient, BlobClient blobClient, String path) {

        when(fileSystem.getContainerClient()).thenReturn(containerClient);
        when(blobClient.exists()).thenReturn(true);
        when(containerClient.getBlobClient(path)).thenReturn(blobClient);

        try {
            Field useCount = AbstractFileSystem.class.getDeclaredField("useCount");
            useCount.setAccessible(true);
            useCount.set(fileSystem, new AtomicLong());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    private BlobProperties blobProperties(long currentTime) {

        Date date = new Date(currentTime);

        OffsetDateTime offsetDateTime = date.toInstant().atOffset(OffsetDateTime.now().getOffset());

        return new BlobProperties(null, offsetDateTime, null, 0, null, null, null, null, null, null
                , null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                                  null, null, null);
    }


    @Test
    public void testGetLastModifiedTime() {

        String path = "test/test.jpg";

        BlobClient blobClient = mock(BlobClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);

        setup(containerClient, blobClient, path);

        AzFileName azFileName = new AzFileName("azbs", "testAccount", "testContainer", path, FileType.FILE);

        AzFileObject target = new AzFileObject(azFileName, fileSystem);

        long currentTime = System.currentTimeMillis();

        when(blobClient.getProperties()).thenReturn(blobProperties(currentTime));

        target.doAttach();

        assertEquals(currentTime, target.doGetLastModifiedTime());
    }


    @Test
    public void testDelete() throws Exception {

        String parentPath = "test/video/";
        String path = parentPath + "video";

        BlobClient blobClient = mock(BlobClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);

        setup(containerClient, blobClient, parentPath);

        AzFileName azFileName = new AzFileName("azbs", "testAccount", "testContainer", path, FileType.IMAGINARY);

        AzFileObject target = new AzFileObject(azFileName, fileSystem);

        PagedIterable pagedIterable = mock(PagedIterable.class);
        Iterator iterator = mock(Iterator.class);

        when(containerClient.listBlobsByHierarchy(path)).thenReturn(pagedIterable);
        when(pagedIterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);
        when(containerClient.getBlobClient(parentPath)).thenReturn(blobClient);

        target.delete();
        verify(containerClient, times(1)).getBlobClient(parentPath);
        verify(blobClient, times(1)).delete();
    }


    @Test
    public void testDelete_DifferentZeroByteFile() throws Exception {

        String parentPath = "test/video/";
        String path = parentPath + "video1";

        BlobClient blobClient = mock(BlobClient.class);
        BlobContainerClient containerClient = mock(BlobContainerClient.class);

        setup(containerClient, blobClient, parentPath);

        AzFileName azFileName = new AzFileName("azbs", "testAccount", "testContainer", path, FileType.IMAGINARY);

        AzFileObject target = new AzFileObject(azFileName, fileSystem);

        PagedIterable pagedIterable = mock(PagedIterable.class);
        Iterator iterator = mock(Iterator.class);

        when(containerClient.listBlobsByHierarchy(path)).thenReturn(pagedIterable);
        when(pagedIterable.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);
        when(containerClient.getBlobClient(parentPath)).thenReturn(blobClient);

        target.delete();
        verify(containerClient, never()).getBlobClient(parentPath);
        verify(blobClient, never()).delete();
    }
}
