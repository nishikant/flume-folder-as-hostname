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

import com.google.common.base.Charsets;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;


/**
 * Created by cto on 9/24/15.
 */
public class EventFolderNameInterceptorTest {

  //TODO: build sample config and test this interceptor
  @Test
  public void testBasic() throws IOException {

    final String fileKey = "pathKey";
    final String folderKey = "testKey";
//final Integer indexKey = 0;
    final String indexKey = "folderIndex";

    Context context = new Context();
    context.put(EventFolderNameInterceptor.Constants.FILEHEADERKEYPROPERTY, fileKey);
    context.put(EventFolderNameInterceptor.Constants.FOLDERNAMEKEYPROPERTY, folderKey);
    context.put(EventFolderNameInterceptor.Constants.FOLDERINDEXKEYPROPERTY, indexKey);

    Interceptor.Builder builder = new EventFolderNameInterceptor.Builder();
    builder.configure(context);
    Interceptor interceptor = builder.build();


    Event event = EventBuilder.withBody(
        "###[1979-07-21 00:00:00]|test event", Charsets.UTF_8);

    event.getHeaders().put(fileKey,"/home/gattu/flume-folder-interceptor/flumeInterceptor/pom.xml");
    event.getHeaders().put(indexKey,"4");
    assertNull(folderKey + " does not exist", event.getHeaders().get(folderKey));
    event = interceptor.intercept(event);
    assertEquals(folderKey + " is set", event.getHeaders().get(folderKey),"flumeInterceptor" );

    event = EventBuilder.withBody(
        "###[1979-07-21 00:00:00]|test event", Charsets.UTF_8);

    event.getHeaders().put(fileKey,"/flumeInterceptor/pom.xml");
    assertNull(folderKey + " does not exist", event.getHeaders().get(folderKey));
    event = interceptor.intercept(event);
    assertEquals(folderKey + " is set", event.getHeaders().get(folderKey),"flumeInterceptor" );

  }

  @Test
  public void testNegative() throws IOException {
    //test no file key expect default
    final String fileKey = "pathKey";
    final String folderKey = "testKey";

    Context context = new Context();
    context.put(EventFolderNameInterceptor.Constants.FILEHEADERKEYPROPERTY, fileKey);
    context.put(EventFolderNameInterceptor.Constants.FOLDERNAMEKEYPROPERTY, folderKey);

    Interceptor.Builder builder = new EventFolderNameInterceptor.Builder();
    builder.configure(context);
    Interceptor interceptor = builder.build();


    Event event = EventBuilder.withBody(
        "###[1979-07-21 00:00:00]|test event", Charsets.UTF_8);
    assertNull(folderKey + " does not exist", event.getHeaders().get(folderKey));
    // Call intercept with no fileKey set
    event = interceptor.intercept(event);
    // expect default folder name
    assertEquals("folder name should be default",EventFolderNameInterceptor.Constants.FOLDERNAME_DFLT,event.getHeaders().get(folderKey));

    // Test as root expect default
    event = EventBuilder.withBody(
        "###[1979-07-21 00:00:00]|test event", Charsets.UTF_8);
    event.getHeaders().put(fileKey,"/pom.xml");
    assertNull(folderKey + " does not exist", event.getHeaders().get(folderKey));
    event = interceptor.intercept(event);
    assertEquals("folder name should be default",EventFolderNameInterceptor.Constants.FOLDERNAME_DFLT,event.getHeaders().get(folderKey));

  }


}
