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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.vfs.Capability;
import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystem;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.UserAuthenticationData;
import org.apache.commons.vfs.provider.AbstractOriginatingFileProvider;

public class HDFSFileProvider extends AbstractOriginatingFileProvider {
  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "hdfs";

  /** User Information. */
  public static final String ATTR_USER_INFO = "UI";

  /** Authentication types. */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] { UserAuthenticationData.USERNAME,
      UserAuthenticationData.PASSWORD };

  /** The provider's capabilities. */
  protected static final Collection<Capability> capabilities = Collections.unmodifiableCollection(Arrays.asList(new Capability[] { Capability.CREATE, Capability.DELETE,
      Capability.RENAME, Capability.GET_TYPE, Capability.LIST_CHILDREN, Capability.READ_CONTENT, Capability.URI, Capability.WRITE_CONTENT, Capability.APPEND_CONTENT,
      Capability.GET_LAST_MODIFIED, Capability.SET_LAST_MODIFIED_FILE, Capability.RANDOM_ACCESS_READ }));

  public HDFSFileProvider() {
    super();
    setFileNameParser(HDFSFileNameParser.getInstance());
  }

  protected FileSystem doCreateFileSystem(final FileName name, final FileSystemOptions fileSystemOptions) throws FileSystemException {
    // Create the file system
    // UserAuthenticationData authData = null;
    // try {
    // //authData = UserAuthenticatorUtils.authenticate(fileSystemOptions, AUTHENTICATOR_TYPES);
    // // UserAuthenticatorUtils.getData(authData, UserAuthenticationData.USERNAME, UserAuthenticatorUtils.toChar(rootName.getUserName())),
    // // UserAuthenticatorUtils.getData(authData, UserAuthenticationData.PASSWORD, UserAuthenticatorUtils.toChar(rootName.getPassword())), fileSystemOptions);
    // } catch (final Exception e) {
    // throw new FileSystemException("vfs.provider.sftp/connect.error", name, e);
    // } finally {
    // UserAuthenticatorUtils.cleanup(authData);
    // }
    //
    return new HDFSFileSystem(name, fileSystemOptions);
  }

  public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
