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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.VFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.hdfs.vfs.HDFSFileSystem;
import org.pentaho.hdfs.vfs.HDFSFileSystemConfigBuilder;

public class HDFSVFSTest {

  private static FileSystemManager fsManager;
  private static String HELLO_HADOOP_STR = "Hello Hadoop VFS";

  private static MiniDFSCluster cluster = null;

  @BeforeClass
  public static void beforeClass() throws IOException {
    fsManager = VFS.getManager();
    final Configuration dfsConf = new Configuration();
    dfsConf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".users", "users");
    dfsConf.set("hadoop.proxyuser.users.ip-addresses", "localhost");
    dfsConf.set("hadoop.proxyuser." + System.getProperty("user.name") + ".ip-addresses", "localhost");
    cluster = new MiniDFSCluster(dfsConf, 2, true, null);
    cluster.waitActive();

    FileSystem hdfs = cluster.getFileSystem();
    HDFSFileSystem.setMockHDFSFileSystem(hdfs);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.shutdown();
  }

  public static String buildHDFSURL(String path) {
    return "hdfs://localhost" + path;
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
  
  @Test
  public void testSetConfig() {
    Configuration config = new Configuration();
    FileSystemOptions opts = new FileSystemOptions();
    HDFSFileSystemConfigBuilder configBuilder = new HDFSFileSystemConfigBuilder();
    String option1 = "option1";
    String value1 = "value1";
    String option2 = "option2";
    String value2 = "value2";
    configBuilder.setParam( opts, option1, value1 );
    configBuilder.setParam( opts, option2, value2 );
    HDFSFileSystem.setFileSystemOptions( opts, config );
    assertEquals( value1, config.get( option1 ) );
    assertEquals( value2, config.get( option2 ) );
  }
}
