/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.hdfs.vfs;

import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.URLFileNameParser;

public class HDFSFileNameParser extends URLFileNameParser {

  private static final HDFSFileNameParser INSTANCE = new HDFSFileNameParser();

  public HDFSFileNameParser() {
    super(-1);
  }

  public static FileNameParser getInstance() {
    return INSTANCE;
  }
}
