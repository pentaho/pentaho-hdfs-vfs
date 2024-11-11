/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hdfs.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.hadoop.conf.Configuration;

/**
 * Handles connecting to a MapR FileSystem. To be used with maprfs:// protocol.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileSystem extends HDFSFileSystem implements FileSystem {
  private HadoopFileSystem fs;

  public MapRFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
    super(rootName, fileSystemOptions);
  }

  @Override
  public HadoopFileSystem getHDFSFileSystem() throws FileSystemException {
    if (fs == null) {
      Configuration conf = new Configuration();
      conf.set("fs.maprfs.impl", MapRFileProvider.FS_MAPR_IMPL);

      GenericFileName rootName = (GenericFileName) getRootName();
      String url = rootName.getScheme() + "://" + rootName.getHostName().trim();
      if (rootName.getPort() != MapRFileNameParser.DEFAULT_PORT) {
        url += ":" + rootName.getPort();
      }
      url += "/";
      conf.set("fs.default.name", url);
      setFileSystemOptions( getFileSystemOptions(), conf );
      try {
        fs = new HadoopFileSystemImpl( org.apache.hadoop.fs.FileSystem.get(conf) );
      } catch (Throwable t) {
        throw new FileSystemException("Could not get MapR FileSystem for " + url, t);
      }
    }
    return fs;
  }
}
