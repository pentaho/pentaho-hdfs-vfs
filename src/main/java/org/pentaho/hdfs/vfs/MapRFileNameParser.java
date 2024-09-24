/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hdfs.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.apache.commons.vfs2.provider.url.UrlFileName;

/**
 * Parses MapR FileSystem URIs. This only differs from {@link URLFileNameParser} in that it allows empty host names.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileNameParser extends URLFileNameParser {
  public static final int DEFAULT_PORT = -1;
  public static final String EMPTY_HOSTNAME = "";

  public MapRFileNameParser() {
    super(DEFAULT_PORT);
  }

  @Override
  protected String extractHostName(StringBuilder name) {
    final String hostname = super.extractHostName(name);
    // Trick the URLFileNameParser into thinking we have a hostname so we don't have to refactor it.
    return hostname == null ? EMPTY_HOSTNAME : hostname;
  }
}
