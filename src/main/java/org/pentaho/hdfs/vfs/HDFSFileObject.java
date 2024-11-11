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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HDFSFileObject extends AbstractFileObject {

  private HadoopFileSystem hdfs;

  protected HDFSFileObject(final AbstractFileName name, final HDFSFileSystem fileSystem) throws FileSystemException {
    super(name, fileSystem);
    hdfs = fileSystem.getHDFSFileSystem();
  }

  @Override
  protected long doGetContentSize() throws Exception {
    return hdfs.getFileStatus(new Path(getName().getPath())).getLen();
  }

  @Override
  protected OutputStream doGetOutputStream(boolean append) throws Exception {
    if (append) {
      FSDataOutputStream out = hdfs.append(new Path(getName().getPath()));
      return out;
    } else {
      FSDataOutputStream out = hdfs.create(new Path(getName().getPath()));
      return out;
    }
  }

  @Override
  protected InputStream doGetInputStream() throws Exception {
    FSDataInputStream in = hdfs.open(new Path(getName().getPath()));
    return in;
  }

  @Override
  protected FileType doGetType() throws Exception {
    FileStatus status = null;
    try {
      status = hdfs.getFileStatus(new Path(URLDecoder.decode(getName().getPath(), "UTF-8").replaceAll(" ", "+")));
    } catch (Exception ex) {
    }

    if (status == null) {
      return FileType.IMAGINARY;
    } else if (status.isDir()) {
      return FileType.FOLDER;
    } else {
      return FileType.FILE;
    }
  }

  @Override
  public void doCreateFolder() throws Exception {
    hdfs.mkdirs(new Path(getName().getPath()));
  }

  @Override
  public void doDelete() throws Exception {
    hdfs.delete(new Path(getName().getPath()), true);
  }

  @Override
  protected void doRename(FileObject newfile) throws Exception {
    hdfs.rename(new Path(getName().getPath()), new Path(newfile.getName().getPath()));
  }

  @Override
  protected long doGetLastModifiedTime() throws Exception {
    return hdfs.getFileStatus(new Path(getName().getPath())).getModificationTime();
  }

  @Override
  protected boolean doSetLastModifiedTime(long modtime) throws Exception {
    hdfs.setTimes(new Path(getName().getPath()), modtime, System.currentTimeMillis());
    return true;
  }

  @Override
  protected String[] doListChildren() throws Exception {
    FileStatus[] statusList = hdfs.listStatus(new Path(getName().getPath()));
    String[] children = new String[statusList.length];
    for (int i = 0; i < statusList.length; i++) {
      children[i] = URLEncoder.encode(statusList[i].getPath().getName(), "UTF-8");
    }
    return children;
  }

}
