package org.pentaho.hdfs.vfs.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.vfs.FileSystemOptions;
import org.junit.Test;
import org.pentaho.hdfs.vfs.HDFSFileSystemConfigBuilder;

public class HDFSFileSystemConfigBuilderTest {
  @Test
  public void testHDFSFileSystemConfigBuilderTracksOptions() {
    FileSystemOptions opts = new FileSystemOptions();
    HDFSFileSystemConfigBuilder configBuilder = new HDFSFileSystemConfigBuilder();
    String option1 = "option1";
    String value1 = "value1";
    String option2 = "option2";
    String value2 = "value2";
    String value3 = "value3";

    assertTrue( "Expected params to start empty", configBuilder.getOptions( opts ).size() == 0 );
    assertFalse( configBuilder.hasParam( opts, option1 ) );
    assertFalse( configBuilder.hasParam( opts, option2 ) );

    configBuilder.setParam( opts, option1, value1 );
    assertTrue( "Expected params to only have option1", configBuilder.getOptions( opts ).size() == 1
        && configBuilder.getOptions( opts ).contains( option1 ) );
    assertEquals( value1, configBuilder.getParam( opts, option1 ) );
    assertTrue( configBuilder.hasParam( opts, option1 ) );
    assertFalse( configBuilder.hasParam( opts, option2 ) );

    configBuilder.setParam( opts, option2, value2 );
    assertTrue( "Expected params to only have option1 and option2", configBuilder.getOptions( opts ).size() == 2
        && configBuilder.getOptions( opts ).contains( option1 ) && configBuilder.getOptions( opts ).contains( option2 ) );
    assertEquals( value1, configBuilder.getParam( opts, option1 ) );
    assertEquals( value2, configBuilder.getParam( opts, option2 ) );
    assertTrue( configBuilder.hasParam( opts, option1 ) );
    assertTrue( configBuilder.hasParam( opts, option2 ) );
    
    configBuilder.setParam( opts, option2, value3 );
    assertTrue( "Expected params to only have option1 and option2", configBuilder.getOptions( opts ).size() == 2
        && configBuilder.getOptions( opts ).contains( option1 ) && configBuilder.getOptions( opts ).contains( option2 ) );
    assertEquals( value1, configBuilder.getParam( opts, option1 ) );
    assertEquals( value3, configBuilder.getParam( opts, option2 ) );
    assertTrue( configBuilder.hasParam( opts, option1 ) );
    assertTrue( configBuilder.hasParam( opts, option2 ) );
  }
}
