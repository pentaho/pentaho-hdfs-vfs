package org.pentaho.hdfs.vfs;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;

/**
 * Provides access to the MapR FileSystem VFS implementation.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileProvider extends HDFSFileProvider {
  public MapRFileProvider() {
    setFileNameParser(new MapRFileNameParser());
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions) throws FileSystemException {
    return new MapRFileSystem(name, fileSystemOptions);
  }
}
