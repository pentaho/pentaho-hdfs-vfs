/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.hdfs.vfs;

import java.util.Collection;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.provider.AbstractFileSystem;
import org.apache.commons.vfs.provider.GenericFileName;
import org.apache.hadoop.conf.Configuration;

public class HDFSFileSystem extends AbstractFileSystem implements FileSystem {

  private static HadoopFileSystem mockHdfs;
  private HadoopFileSystem hdfs;

  protected HDFSFileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected void addCapabilities(Collection caps) {
    caps.addAll(HDFSFileProvider.capabilities);
  }

  @Override
  protected FileObject createFile(FileName name) throws Exception {
    return new HDFSFileObject(name, this);
  }

  
  /**
   * Use of this method is for unit testing, it allows us to poke in a test filesystem without
   * interfering with any other objects in the vfs system
   * 
   * @param hdfs the mock file system
   */
  public static void setMockHDFSFileSystem(org.apache.hadoop.fs.FileSystem hdfs) {
    mockHdfs = new HadoopFileSystemImpl( hdfs );
  }
  
  public HadoopFileSystem getHDFSFileSystem() throws FileSystemException {
    if (mockHdfs != null) {
      return mockHdfs;
    }
    if (hdfs == null) {
      Configuration conf = new Configuration();
      GenericFileName genericFileName = (GenericFileName) getRootName();
      StringBuffer urlBuffer = new StringBuffer("hdfs://");
      urlBuffer.append(genericFileName.getHostName());
      int port = genericFileName.getPort();
      if(port >= 0) {
        urlBuffer.append(":");
        urlBuffer.append(port);
      }
      String url = urlBuffer.toString();
      conf.set("fs.default.name", url);

      String replication = System.getProperty("dfs.replication", "3");
      conf.set("dfs.replication", replication);

      if (genericFileName.getUserName() != null && !"".equals(genericFileName.getUserName())) {
        conf.set("hadoop.job.ugi", genericFileName.getUserName() + ", " + genericFileName.getPassword());
      }
      setFileSystemOptions( getFileSystemOptions(), conf );
      try {
        hdfs = new HadoopFileSystemImpl( org.apache.hadoop.fs.FileSystem.get( conf ) );
      } catch (Throwable t) {
        throw new FileSystemException("Could not getHDFSFileSystem() for " + url, t);
      }
    }
    return hdfs;
  }

  public static void setFileSystemOptions( FileSystemOptions fileSystemOptions, Configuration configuration ) {
    HDFSFileSystemConfigBuilder hdfsFileSystemConfigBuilder = new HDFSFileSystemConfigBuilder();
    FileSystemOptions opts = fileSystemOptions;
    for ( String name : hdfsFileSystemConfigBuilder.getOptions( opts ) ) {
      if ( hdfsFileSystemConfigBuilder.hasParam( opts, name ) ) {
        configuration.set( name, String.valueOf( hdfsFileSystemConfigBuilder.getParam( opts, name ) ) );
      }
    }
  }
}
