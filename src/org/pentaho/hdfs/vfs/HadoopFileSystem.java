package org.pentaho.hdfs.vfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public interface HadoopFileSystem {

  public abstract FSDataOutputStream append( Path path ) throws IOException;

  public abstract FSDataOutputStream create( Path path ) throws IOException;

  public abstract boolean delete( Path path, boolean arg1 ) throws IOException;

  public abstract FileStatus getFileStatus( Path path ) throws IOException;

  public abstract boolean mkdirs( Path path ) throws IOException;

  public abstract FSDataInputStream open( Path path ) throws IOException;

  public abstract boolean rename( Path path, Path path2 ) throws IOException;

  public abstract void setTimes( Path path, long mtime, long atime ) throws IOException;

  public abstract FileStatus[] listStatus( Path path ) throws IOException;

}
