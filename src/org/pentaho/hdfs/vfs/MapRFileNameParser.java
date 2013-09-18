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
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.provider.FileNameParser;
import org.apache.commons.vfs.provider.URLFileNameParser;
import org.apache.commons.vfs.provider.VfsComponentContext;
import org.apache.commons.vfs.provider.url.UrlFileName;

/**
 * Parses MapR FileSystem URIs. This only differs from {@link URLFileNameParser} in that it allows empty host names.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileNameParser extends URLFileNameParser {
  public static final int DEFAULT_PORT = -1;
  public static final String EMPTY_HOSTNAME = "";

  public MapRFileNameParser() {
    super(DEFAULT_PORT);
  }

  @Override
  protected String extractHostName(StringBuffer name) {
    final String hostname = super.extractHostName(name);
    // Trick the URLFileNameParser into thinking we have a hostname so we don't have to refactor it.
    return hostname == null ? EMPTY_HOSTNAME : hostname;
  }
}
