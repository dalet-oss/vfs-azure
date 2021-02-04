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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.commons.vfs2.provider.url.UrlFileNameParser;


/**
 * Used for defining / parsing a provided FileName object.
 *
 * This name should adhere to a URL structure, complete with an 'authority'
 *
 * &lt;scheme&gt;://&lt;host_or_authority&gt;/&lt;container&gt;/&lt;file_path&gt;
 * E.g. azsb://myAccount.blob.core.windows.net/myContainer/path/to/file.txt
 *
 */
public class AzFileNameParser extends UrlFileNameParser {

    private static final AzFileNameParser INSTANCE = new AzFileNameParser();


    private AzFileNameParser() { }

    public static FileNameParser getInstance()
    {
        return INSTANCE;
    }


    @Override
    public FileName parseUri(final VfsComponentContext context, final FileName base, final String uri)
            throws FileSystemException {

        // azbs://flexqaooflexstorage.blob.core.windows.net/flex-azure-vfs-test/test01.tmp

        StringBuilder sb = new StringBuilder(uri);

        UriParser.normalisePath(sb);

        String normalizedUri = sb.toString();
        String scheme = normalizedUri.substring(0, normalizedUri.indexOf(':'));

        String[] s = normalizedUri.split("/");
        String account = s[1].substring(0, s[1].indexOf('.'));
        String container = s[2];

        String absPath = "/";

        for (int i = 3; i < s.length; i++) {

            absPath += s[i];

            if (s.length > 1 && i != s.length - 1) {
                absPath += "/";
            }
        }

        FileType fileType = FileType.IMAGINARY;

        if (uri.endsWith("/")) {
            fileType = FileType.FOLDER;
        }
        else if (!absPath.endsWith("/")) {
            fileType = FileType.FILE;
        }

        return new AzFileName(scheme, account, container, absPath, fileType);
    }

}
