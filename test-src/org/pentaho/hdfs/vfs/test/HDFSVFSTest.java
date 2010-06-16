/*
 * Copyright 2010 Pentaho Corporation.  All rights reserved.
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
 * @author Michael D'Amour
 */
package org.pentaho.hdfs.vfs.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.VFS;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HDFSVFSTest {

  private static FileSystemManager fsManager;
  private static String HELLO_HADOOP_STR = "Hello Hadoop VFS";

  private static String hostname = "192.168.1.193";
  private static String port = "9000";
  private static String username = "username";
  private static String password = "password";

  @BeforeClass
  public static void beforeClass() throws IOException {
    fsManager = VFS.getManager();
    Properties settings = new Properties();
    settings.load(HDFSVFSTest.class.getResourceAsStream("/test-settings.properties"));
    hostname = settings.getProperty("hostname", hostname);
    port = settings.getProperty("port", port);
    username = settings.getProperty("username", username);
    password = settings.getProperty("password", password);
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  public static String buildHDFSURL(String path) {
    // hdfs://myusername:mypassword@somehost/pub/downloads/somefile.tgz
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    if (username != null && !"".equals(username)) {
      return "hdfs://" + username + ":" + password + "@" + hostname + ":" + port + path;
    }
    return "hdfs://" + hostname + ":" + port + path;
  }

  @Test
  public void readFile() throws Exception {
    assertNotNull("FileSystemManager is null", fsManager);

    FileObject hdfsFileOut = fsManager.resolveFile(buildHDFSURL("/junit/file.txt"));
    OutputStream out = hdfsFileOut.getContent().getOutputStream();
    out.write(HELLO_HADOOP_STR.getBytes("UTF-8"));
    out.close();

    FileObject file = fsManager.resolveFile(buildHDFSURL("/junit/file.txt"));
    assertNotNull("File is null (could not resolve?)", file);
    String fileStr = IOUtils.toString(file.getContent().getInputStream(), "UTF-8");
    assertEquals(HELLO_HADOOP_STR, fileStr);

    file.delete();
  }

  @Test
  public void writeFile() throws Exception {
    assertNotNull("FileSystemManager is null", fsManager);
    FileObject file = fsManager.resolveFile(buildHDFSURL("/junit/out.txt"));
    assertEquals(FileType.IMAGINARY, file.getType());
    assertNotNull("File is null (could not resolve?)", file);

    OutputStream output = file.getContent().getOutputStream();
    IOUtils.write(HELLO_HADOOP_STR, output);
    IOUtils.closeQuietly(output);
    assertEquals(FileType.FILE, file.getType());

    String fileStr = IOUtils.toString(file.getContent().getInputStream(), "UTF-8");
    assertEquals(HELLO_HADOOP_STR, fileStr);

    file.delete();
    file = fsManager.resolveFile(buildHDFSURL("/junit/out.txt"));
    assertEquals(FileType.IMAGINARY, file.getType());
  }

  @Test
  public void deleteFile() throws Exception {
    assertNotNull("FileSystemManager is null", fsManager);
    FileObject file = fsManager.resolveFile(buildHDFSURL("/junit/out.txt"));
    assertNotNull("File is null (could not resolve?)", file);
    assertEquals(FileType.IMAGINARY, file.getType());

    OutputStream output = file.getContent().getOutputStream();
    IOUtils.write(HELLO_HADOOP_STR, output);
    IOUtils.closeQuietly(output);
    assertEquals(FileType.FILE, file.getType());

    String fileStr = IOUtils.toString(file.getContent().getInputStream(), "UTF-8");
    assertEquals(HELLO_HADOOP_STR, fileStr);
    file.delete();

    file = fsManager.resolveFile(buildHDFSURL("/junit/out.txt"));
    assertEquals(FileType.IMAGINARY, file.getType());
  }

  @Test
  public void createFolder() throws Exception {
    assertNotNull("FileSystemManager is null", fsManager);
    FileObject folder = fsManager.resolveFile(buildHDFSURL("/junit/folder"));
    assertNotNull("File is null (could not resolve?)", folder);
    assertEquals(FileType.IMAGINARY, folder.getType());
    folder.createFolder();

    folder = fsManager.resolveFile(buildHDFSURL("/junit/folder"));
    assertNotNull("File is null (could not resolve?)", folder);
    assertEquals(FileType.FOLDER, folder.getType());

    folder = fsManager.resolveFile(buildHDFSURL("/junit/folder"));
    assertNotNull("File is null (could not resolve?)", folder);
    assertEquals(FileType.FOLDER, folder.getType());
    assertEquals(true, folder.delete());

    folder = fsManager.resolveFile(buildHDFSURL("/junit/folder"));
    assertNotNull("File is null (could not resolve?)", folder);
    assertEquals(FileType.IMAGINARY, folder.getType());
  }

  @Test
  public void renameFile() throws Exception {
    assertNotNull("FileSystemManager is null", fsManager);

    FileObject file = fsManager.resolveFile(buildHDFSURL("/junit/name.txt"));
    assertNotNull("File is null (could not resolve?)", file);
    assertEquals(FileType.IMAGINARY, file.getType());

    OutputStream output = file.getContent().getOutputStream();
    IOUtils.write(HELLO_HADOOP_STR, output);
    IOUtils.closeQuietly(output);
    assertEquals(FileType.FILE, file.getType());

    FileObject renamedFile = fsManager.resolveFile(buildHDFSURL("/junit/renamed.txt"));
    assertNotNull("File is null (could not resolve?)", renamedFile);
    assertEquals(FileType.IMAGINARY, renamedFile.getType());

    file.moveTo(renamedFile);
    renamedFile = fsManager.resolveFile(buildHDFSURL("/junit/renamed.txt"));
    assertNotNull("File is null (could not resolve?)", renamedFile);
    assertEquals(FileType.FILE, renamedFile.getType());
    assertEquals(true, renamedFile.delete());
  }
  
  @Test
  public void listChildren() throws Exception {
    assertNotNull("FileSystemManager is null", fsManager);

    FileObject folder = fsManager.resolveFile(buildHDFSURL("/junit/folder"));
    assertNotNull("File is null (could not resolve?)", folder);
    assertEquals(FileType.IMAGINARY, folder.getType());
    folder.createFolder();

    folder = fsManager.resolveFile(buildHDFSURL("/junit/folder"));
    assertEquals(FileType.FOLDER, folder.getType());

    FileObject child1 = fsManager.resolveFile(buildHDFSURL("/junit/folder/child1.txt"));
    assertNotNull("File is null (could not resolve?)", child1);
    OutputStream output = child1.getContent().getOutputStream();
    IOUtils.write(HELLO_HADOOP_STR, output);
    IOUtils.closeQuietly(output);
    String fileStr = IOUtils.toString(child1.getContent().getInputStream(), "UTF-8");
    assertEquals(HELLO_HADOOP_STR, fileStr);

    FileObject child2 = fsManager.resolveFile(buildHDFSURL("/junit/folder/child2.txt"));
    assertNotNull("File is null (could not resolve?)", child2);
    output = child2.getContent().getOutputStream();
    IOUtils.write(HELLO_HADOOP_STR, output);
    IOUtils.closeQuietly(output);
    fileStr = IOUtils.toString(child2.getContent().getInputStream(), "UTF-8");
    assertEquals(HELLO_HADOOP_STR, fileStr);

    FileObject[] children = folder.getChildren();
    assertEquals(2, children.length);
    assertEquals("/junit/folder/child1.txt", children[0].getName().getPath());
    assertEquals("/junit/folder/child2.txt", children[1].getName().getPath());

    child1.delete();
    child2.delete();
    folder.delete();
  }
}
