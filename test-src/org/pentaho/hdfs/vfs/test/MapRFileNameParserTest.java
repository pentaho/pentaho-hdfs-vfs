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

package org.pentaho.hdfs.vfs.test;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.provider.FileNameParser;
import org.junit.Test;
import org.pentaho.hdfs.vfs.MapRFileNameParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for parsing MapR FileSystem URIs.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public class MapRFileNameParserTest {

  @Test
  public void rootPathNoClusterName() throws FileSystemException {
    final String URI = "maprfs:///";

    FileNameParser parser = new MapRFileNameParser();
    FileName name = parser.parseUri(null, null, URI);

    assertEquals(URI, name.getURI());
    assertEquals("maprfs", name.getScheme());
  }

  @Test
  public void withPath() throws FileSystemException
  {
    final String URI = "maprfs:///my/file/path";

    FileNameParser parser = new MapRFileNameParser();
    FileName name = parser.parseUri(null, null, URI);

    assertEquals(URI, name.getURI());
    assertEquals("maprfs", name.getScheme());
    assertEquals("/my/file/path", name.getPath());
  }

  @Test
  public void withPathAndClusterName() throws FileSystemException {
    final String URI = "maprfs://cluster2/my/file/path";

    FileNameParser parser = new MapRFileNameParser();
    FileName name = parser.parseUri(null, null, URI);

    assertEquals(URI, name.getURI());
    assertEquals("maprfs", name.getScheme());
    assertTrue(name.getURI().startsWith("maprfs://cluster2/"));
    assertEquals("/my/file/path", name.getPath());
  }
}
