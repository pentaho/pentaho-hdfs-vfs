/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.hdfs.vfs.test;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.junit.Before;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.hdfs.vfs.MapRFileNameParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import org.mockito.MockedStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
/**
 * Tests for parsing MapR FileSystem URIs.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
@RunWith( MockitoJUnitRunner.class )
@Ignore
public class MapRFileNameParserTest {

  private static final String PREFIX = "maprfs";
  private static final String BASE_PATH = "//";
  private static final String BASE_URI = PREFIX + ":" + BASE_PATH;

  private StandardFileSystemManager fsm;
  MockedStatic<UriParser> mockedParser;
  MockedStatic<VFS> mockedStatic;

  @Before
  public void setUp() throws Exception {
    mockedStatic = mockStatic(VFS.class);
    mockedParser = mockStatic(UriParser.class);
    spy( UriParser.class );

    fsm = mock(StandardFileSystemManager.class);
    mockedStatic.when( () -> VFS.getManager()).thenReturn(fsm);
  }

  @After
  public void close() {
    mockedStatic.close();
    mockedParser.close();
  }

  @Test
  public void rootPathNoClusterName() throws Exception {
    final String FILEPATH = "/";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = new MapRFileNameParser();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( PREFIX, name.getScheme() );
  }

  @Test
  public void withPath() throws Exception  {
    final String FILEPATH = "/my/file/path";
    final String URI = BASE_URI + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + FILEPATH );

    FileNameParser parser = new MapRFileNameParser();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( PREFIX, name.getScheme() );
    assertEquals( FILEPATH, name.getPath() );
  }

  @Test
  public void withPathAndClusterName() throws Exception {
    final String HOST = "cluster2";
    final String FILEPATH = "/my/file/path";
    final String URI = BASE_URI + HOST + FILEPATH;

    buildExtractSchemeMocks( PREFIX, URI, BASE_PATH + HOST + FILEPATH );

    FileNameParser parser = new MapRFileNameParser();
    FileName name = parser.parseUri( null, null, URI );

    assertEquals( URI, name.getURI() );
    assertEquals( PREFIX, name.getScheme() );
    assertTrue( name.getURI().startsWith( PREFIX + ":" + BASE_PATH + HOST ) );
    assertEquals( FILEPATH, name.getPath() );
  }

  private Answer buildSchemeAnswer( String prefix, String buildPath ) {
    Answer extractSchemeAnswer = invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[2] ).append( buildPath );
      return prefix;
    };
    return extractSchemeAnswer;
  }

  private void buildExtractSchemeMocks( String prefix, String fullPath, String pathWithoutPrefix ) throws Exception {
    String[] schemes = { "maprfs" };
    when( fsm.getSchemes() ).thenReturn( schemes );
    when( UriParser.extractScheme( eq ( schemes ), eq ( fullPath ), any( StringBuilder.class ) ) ).thenAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) );
  }
}
