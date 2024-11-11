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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

public class HDFSFileSystemConfigBuilder extends FileSystemConfigBuilder {
  private static final String SET_OPTIONS_PARAM = "PENTAHO_SET_OPTIONS";

  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return FileSystem.class;
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
