package org.apache.avro.ipc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * see {@link org.apache.avro.ipc.SocketTransceiver}.
 * 
 * Extract data from ChannelBuffer and composite as List<ByteBuffer> for
 * Avro Responder.
 * 
 * @author why
 * @version $Date:2010-06-20 $
 */
public class NettyFrameDecoder extends FrameDecoder {
	// result needed by Avro Responder
	private List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws Exception {
		
		if (buffer.readableBytes()<4) {
			return null;
		}
		
		buffer.markReaderIndex();
		
		int length = buffer.readInt();
		if (length==0) { // meet ByteBuffer List terminator
			List<ByteBuffer> ret = buffers;
			buffers = new ArrayList<ByteBuffer>();
			return ret;
		}
		
		if (buffer.readableBytes()<length) {
			buffer.resetReaderIndex();
			return null;
		}
		
		ByteBuffer bb = ByteBuffer.allocate(length);
		buffer.readBytes(bb);
		bb.flip();
		buffers.add(bb);
		
		return null;
	}

}
