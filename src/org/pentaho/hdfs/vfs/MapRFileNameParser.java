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
