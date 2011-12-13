package org.pentaho.hdfs.vfs;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.GenericFileName;
import org.apache.hadoop.conf.Configuration;

/**
 * Handles connecting to a MapR FileSystem. To be used with maprfs:// protocol.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileSystem extends HDFSFileSystem implements FileSystem {
  private org.apache.hadoop.fs.FileSystem fs;

  public MapRFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
    super(rootName, fileSystemOptions);
  }

  @Override
  public org.apache.hadoop.fs.FileSystem getHDFSFileSystem() throws FileSystemException {
    if (fs == null) {
      Configuration conf = new Configuration();
      conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");

      GenericFileName rootName = (GenericFileName) getRootName();
      String url = rootName.getScheme() + "://" + rootName.getHostName().trim();
      if (rootName.getPort() != MapRFileNameParser.DEFAULT_PORT) {
        url += ":" + rootName.getPort();
      }
      url += "/";
      conf.set("fs.default.name", url);

      try {
        fs = org.apache.hadoop.fs.FileSystem.get(conf);
      } catch (Throwable t) {
        throw new FileSystemException("Could not get MapR FileSystem for " + url, t);
      }
    }
    return fs;
  }
}
