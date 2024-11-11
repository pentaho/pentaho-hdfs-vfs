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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HadoopFileSystemImpl extends FileSystem implements HadoopFileSystem {
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

  /**
   * Append to an existing file (optional operation).
   *
   * @param f          the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress   for reporting progress if it is not null.
   * @throws java.io.IOException
   */
  @Override
  public FSDataOutputStream append( Path f, int bufferSize, Progressable progress ) throws IOException {
    return delegate.append(f, bufferSize, progress);
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

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress reporting.
   *
   * @param f           the file name to open
   * @param permission
   * @param overwrite   if a file with this name already exists, then if true, the file will be overwritten, and if
   *                    false an error will be thrown.
   * @param bufferSize  the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws java.io.IOException
   * @see #setPermission(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.permission.FsPermission)
   */
  @Override
  public FSDataOutputStream create( Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                              short replication, long blockSize, Progressable progress )
    throws IOException {
    return delegate.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
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

  /**
   * Set the current working directory for the given file system. All relative paths will be resolved relative to it.
   *
   * @param new_dir
   */
  @Override
  public void setWorkingDirectory( Path new_dir ) {
    delegate.setWorkingDirectory( new_dir );
  }

  /**
   * Get the current working directory for the given file system
   *
   * @return the directory pathname
   */
  @Override
  public Path getWorkingDirectory() {
    return delegate.getWorkingDirectory();
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

  /**
   * Make the given file and all non-existent parents into directories. Has the semantics of Unix 'mkdir -p'. Existence
   * of the directory hierarchy is not an error.
   *
   * @param f          path to create
   * @param permission to apply to f
   */
  @Override
  public boolean mkdirs( Path f, FsPermission permission ) throws IOException {
    return delegate.mkdirs( f, permission );
  }

  /**
   * Returns a URI whose scheme and authority identify this FileSystem.
   */
  @Override
  public URI getUri() {
    return delegate.getUri();
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f          the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open( Path f, int bufferSize ) throws IOException {
    return delegate.open( f, bufferSize );
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

  /**
   * @param f
   * @deprecated Use delete(Path, boolean) instead
   */
  @Override
  public boolean delete(Path f) throws IOException {
    return delete( f , true );
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
