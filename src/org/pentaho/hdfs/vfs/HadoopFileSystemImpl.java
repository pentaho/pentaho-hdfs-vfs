package org.pentaho.hdfs.vfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileSystemImpl implements HadoopFileSystem {
  private final FileSystem delegate;

  public HadoopFileSystemImpl( FileSystem delegate ) {
    this.delegate = delegate;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#append(org.apache.hadoop.fs.Path)
   */
  @Override
  public FSDataOutputStream append( Path f ) throws IOException {
    return delegate.append( f );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#create(org.apache.hadoop.fs.Path)
   */
  @Override
  public FSDataOutputStream create( Path f ) throws IOException {
    return delegate.create( f );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#delete(org.apache.hadoop.fs.Path, boolean)
   */
  @Override
  public boolean delete( Path arg0, boolean arg1 ) throws IOException {
    return delegate.delete( arg0, arg1 );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#getFileStatus(org.apache.hadoop.fs.Path)
   */
  @Override
  public FileStatus getFileStatus( Path arg0 ) throws IOException {
    return delegate.getFileStatus( arg0 );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#listStatus(org.apache.hadoop.fs.Path)
   */
  @Override
  public FileStatus[] listStatus( Path arg0 ) throws IOException {
    return delegate.listStatus( arg0 );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#mkdirs(org.apache.hadoop.fs.Path)
   */
  @Override
  public boolean mkdirs( Path f ) throws IOException {
    return delegate.mkdirs( f );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#open(org.apache.hadoop.fs.Path)
   */
  @Override
  public FSDataInputStream open( Path f ) throws IOException {
    return delegate.open( f );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#rename(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path)
   */
  @Override
  public boolean rename( Path arg0, Path arg1 ) throws IOException {
    return delegate.rename( arg0, arg1 );
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.hdfs.vfs.HadoopFileSystem#setTimes(org.apache.hadoop.fs.Path, long, long)
   */
  @Override
  public void setTimes( Path p, long mtime, long atime ) throws IOException {
    delegate.setTimes( p, mtime, atime );
  }
}
