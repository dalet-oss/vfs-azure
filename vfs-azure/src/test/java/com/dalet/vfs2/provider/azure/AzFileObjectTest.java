package com.dalet.vfs2.provider.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobProperties;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AzFileObjectTest {

    private final static AbstractFileName fileName = mock(AbstractFileName.class);
    private final static AzFileSystem fileSystem = mock(AzFileSystem.class);
    private final static BlobClient blobClient = mock(BlobClient.class);
    private final AzFileObject target = new AzFileObject(fileName, fileSystem);


    @BeforeClass
    public static void setup() {

        BlobContainerClient containerClient = mock(BlobContainerClient.class);

        when(fileSystem.getContainerClient()).thenReturn(containerClient);
        when(blobClient.exists()).thenReturn(true);
        when(fileName.getPath()).thenReturn("test/test.jpg");
        when(containerClient.getBlobClient("test/test.jpg")).thenReturn(blobClient);

        Field useCount = ReflectionUtils.findField(AzFileSystem.class, "useCount");
        assert useCount != null;
        useCount.setAccessible(true);
        ReflectionUtils.setField(useCount, fileSystem, new AtomicLong());
    }


    private BlobProperties blobProperties(long currentTime) {

        Date date = new Date(currentTime);

        OffsetDateTime offsetDateTime = date.toInstant().atOffset(OffsetDateTime.now().getOffset());

        return new BlobProperties(null, offsetDateTime, null, 0, null, null, null, null, null, null
                , null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                                  null, null, null);
    }


    @Test
    public void test() {

        long currentTime = System.currentTimeMillis();

        when(blobClient.getProperties()).thenReturn(blobProperties(currentTime));

        target.doAttach();

        assertEquals(currentTime, target.doGetLastModifiedTime());
    }

}
