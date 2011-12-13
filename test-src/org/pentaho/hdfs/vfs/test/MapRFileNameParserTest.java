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
