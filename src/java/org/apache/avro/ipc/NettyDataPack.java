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

package org.apache.avro.ipc;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Transport protocol data structure when using Netty. 
 * 
 * @author why
 * @version $Date:2010-06-27 $
 */
public class NettyDataPack {
  private int serial;
  private List<ByteBuffer> datas;

  public NettyDataPack() {}
  
  public NettyDataPack(int serial, List<ByteBuffer> datas) {
    this.serial = serial;
    this.datas = datas;
  }
  
  public void setSerial(int serial) {
    this.serial = serial;
  }

  public int getSerial() {
    return serial;
  }
  
  public void setDatas(List<ByteBuffer> datas) {
    this.datas = datas;
  }

  public List<ByteBuffer> getDatas() {
    return datas;
  }
  
}
