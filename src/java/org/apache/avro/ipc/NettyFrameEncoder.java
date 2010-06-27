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
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * Protocol encoder which converts NettyDataPack which contains the 
 * Responder's output List&lt;ByteBuffer&gt; to ChannelBuffer needed 
 * by Netty.
 * 
 * @author why
 * @version $Date:2010-06-20 $
 */
public class NettyFrameEncoder extends OneToOneEncoder {

  /**
   * encode msg to ChannelBuffer
   * @param msg NettyDataPack from 
   *            NettyServerAvroHandler/NettyClientAvroHandler in the pipeline
   * @return encoded ChannelBuffer
   */
  @Override
  protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
      throws Exception {
    NettyDataPack dataPack = (NettyDataPack)msg;
    List<ByteBuffer> origs = dataPack.getDatas();
    List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(origs.size() * 2 + 1);
    bbs.add(getPackHeader(dataPack)); // prepend a pack header including serial number and list size
    for (ByteBuffer b : origs) {
      bbs.add(getLengthHeader(b)); // for each buffer prepend length field
      bbs.add(b);
    }

    return ChannelBuffers
        .wrappedBuffer(bbs.toArray(new ByteBuffer[bbs.size()]));
  }
  
  private ByteBuffer getPackHeader(NettyDataPack dataPack) {
    ByteBuffer header = ByteBuffer.allocate(8);
    header.putInt(dataPack.getSerial());
    header.putInt(dataPack.getDatas().size());
    header.flip();
    return header;
  }

  private ByteBuffer getLengthHeader(ByteBuffer buf) {
    ByteBuffer header = ByteBuffer.allocate(4);
    header.putInt(buf.limit());
    header.flip();
    return header;
  }
}
