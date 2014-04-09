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
