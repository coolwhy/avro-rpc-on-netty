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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * Protocol decoder which converts Netty's ChannelBuffer to 
 * NettyDataPack which contains a List&lt;ByteBuffer&gt; needed 
 * by Avro Responder.
 * 
 * @author why
 * @version $Date:2010-06-20 $
 */
public class NettyFrameDecoder extends FrameDecoder {
  private boolean packHeaderRead = false;
  private int listSize;
  private NettyDataPack dataPack;
  
  /**
   * decode buffer to NettyDataPack
   */
  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel,
      ChannelBuffer buffer) throws Exception {

    if (!packHeaderRead) {
      if (decodePackHeader(ctx, channel, buffer)) {
        packHeaderRead = true;
      }
      return null;
    } else {
      if (decodePackBody(ctx, channel, buffer)) {
        packHeaderRead = false; // reset state
        return dataPack;
      } else {
        return null;
      }
    }
    
  }
  
  private boolean decodePackHeader(ChannelHandlerContext ctx, Channel channel,
      ChannelBuffer buffer) throws Exception {
    if (buffer.readableBytes()<8) {
      return false;
    }

    int serial = buffer.readInt();
    listSize = buffer.readInt();
    dataPack = new NettyDataPack(serial, new ArrayList<ByteBuffer>(listSize));
    return true;
  }
  
  private boolean decodePackBody(ChannelHandlerContext ctx, Channel channel,
      ChannelBuffer buffer) throws Exception {
    if (buffer.readableBytes() < 4) {
      return false;
    }

    buffer.markReaderIndex();
    
    int length = buffer.readInt();

    if (buffer.readableBytes() < length) {
      buffer.resetReaderIndex();
      return false;
    }

    ByteBuffer bb = ByteBuffer.allocate(length);
    buffer.readBytes(bb);
    bb.flip();
    dataPack.getDatas().add(bb);
    
    return dataPack.getDatas().size()==listSize;
  }

}
