package com.dalet.vfs2.provider.azure;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;


public class AzFileName extends AbstractFileName {

    private final String account;
    private final String container;


    public AzFileName(String scheme, String account, String container, String path, FileType type) {

        super(scheme, path, type);

        this.account = account;
        this.container = container;
    }


    public String getAccount() {
        return account;
    }


    public String getContainer() {
        return container;
    }


    /**
     * Factory method for creating name instances.
     *
     * @param absolutePath The absolute path.
     * @param fileType     The FileType.
     * @return The FileName.
     */
    @Override
    public FileName createName(String absolutePath, FileType fileType) {

        return new AzFileName(getScheme(), this.account, this.container, absolutePath, fileType);
    }


    /**
     * Builds the root URI for this file name. Note that the root URI must not end with a separator character.
     *
     * Azure URIs take the following form:
     *
     * azbs://[storage_account_name].blob.core.windows.net/[container_name]/[file_path]
     *
     * @param buffer      A StringBuilder to use to construct the URI.
     * @param addPassword true if the password should be added, false otherwise.
     */
    @Override
    protected void appendRootUri(StringBuilder buffer, boolean addPassword) {

        buffer.append(getScheme());
        buffer.append("://");
        buffer.append(account);
        buffer.append(".blob.core.windows.net/");
        buffer.append(container);
    }

}
