/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.interceptor.EventFolderNameInterceptor.Constants.*;

/**
 *
 * Simple Interceptor class that extract the last folder name from the given fileHeaderKey .
 * and set it to the given target folder key
 * @author Eran Witkon (eranwitkon@gmail.com)
 *
 */

public class EventFolderNameInterceptor implements Interceptor {
  private static final Logger logger = LoggerFactory.getLogger(EventFolderNameInterceptor.class);
  private final String folderNameKey;
  private final String fileHeaderKey;
  private final String folderIndexKey;

  // private constructor - initilized only by the builder
  private EventFolderNameInterceptor(String fileHeaderKey,String targetFolderName, String folderIndexKey) {
    this.fileHeaderKey = fileHeaderKey;
    this.folderNameKey = targetFolderName;
    this.folderIndexKey = folderIndexKey;
  }

  public void initialize() {
    // no-op
  }

  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    headers.put(folderNameKey,FOLDERNAME_DFLT);
    if (headers.containsKey(fileHeaderKey) && headers.containsKey(folderIndexKey)) {
      // extract the last folder name from the given key
      File file = new File(headers.get(fileHeaderKey));
      String path = file.getAbsolutePath();
      String[] paths = path.split("/");

      //String parent = file.getParent();
      if (paths != null) {
        String dir = paths[Integer.parseInt(headers.get(folderIndexKey))];
        if (!dir.isEmpty()) {
          headers.put(folderNameKey, dir);
        }
      }
    }
    return event;
  }

  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   *
   * @param events
   * @return
   */
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instances of the EventFolderNameInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private String folderNameKey = FOLDERNAMEKEY_DFLT;
    private String fileHeaderKey = FILEHEADERKEY_DFLT;
    private String folderIndexKey = FOLDERINDEXKEY_DFLT;

    public Interceptor build() {
      return new EventFolderNameInterceptor(fileHeaderKey, folderNameKey, folderIndexKey );
    }

    public void configure(Context context) {
      folderNameKey = context.getString(FOLDERNAMEKEYPROPERTY, folderNameKey);
      fileHeaderKey = context.getString(FILEHEADERKEYPROPERTY, fileHeaderKey);
      folderIndexKey = context.getString(FOLDERINDEXKEYPROPERTY, folderIndexKey);
    }
  }

  public static class Constants {
    // this is the given path name key
    public static String FILEHEADERKEYPROPERTY = "fileHeaderKey";
    // this is the default path name key
    public static String FILEHEADERKEY_DFLT = "file";
    // this is the folder key name
    public static String FOLDERNAMEKEYPROPERTY = "targetFolderKey";
    // this is the default folder key
    public static String FOLDERNAMEKEY_DFLT = "defaultFolderKey";
    // this is the name of the folder that will be returned  if not found
    public static String FOLDERNAME_DFLT = "dfltFolder";
    // this is the folder index key name
    public static String FOLDERINDEXKEYPROPERTY  = "hostFolderKey";
    // By default first folder will be returned  if not found
    public static String FOLDERINDEXKEY_DFLT = "0";
  }

}
