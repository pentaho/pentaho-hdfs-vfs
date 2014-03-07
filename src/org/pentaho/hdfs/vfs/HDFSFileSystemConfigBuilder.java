package org.pentaho.hdfs.vfs;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.vfs.FileSystemConfigBuilder;
import org.apache.commons.vfs.FileSystemOptions;

public class HDFSFileSystemConfigBuilder extends FileSystemConfigBuilder {
  private static final String SET_OPTIONS_PARAM = "PENTAHO_SET_OPTIONS";

  @Override
  protected Class<HDFSFileSystemConfigBuilder> getConfigClass() {
    return HDFSFileSystemConfigBuilder.class;
  }

  @Override
  public void setParam( FileSystemOptions opts, String name, Object value ) {
    if ( !hasParam( opts, name ) ) {
      if ( hasParam( opts, SET_OPTIONS_PARAM ) ) {
        super.setParam( opts, SET_OPTIONS_PARAM, getParam( opts, SET_OPTIONS_PARAM ) + "," + name );
      } else {
        super.setParam( opts, SET_OPTIONS_PARAM, name );
      }
    }
    super.setParam( opts, name, value );
  }

  @Override
  public Object getParam( FileSystemOptions opts, String name ) {
    return super.getParam( opts, name );
  }

  @Override
  public boolean hasParam( FileSystemOptions opts, String name ) {
    return super.hasParam( opts, name );
  }

  @SuppressWarnings( "unchecked" )
  public Set<String> getOptions( FileSystemOptions opts ) {
    if ( hasParam( opts, SET_OPTIONS_PARAM ) ) {
      return new HashSet<String>( Arrays.asList( ( (String) getParam( opts, SET_OPTIONS_PARAM ) ).split( "," ) ) );
    } else {
      return Collections.EMPTY_SET;
    }
  }
}
