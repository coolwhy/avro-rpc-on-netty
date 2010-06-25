package org.apache.avro.ipc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * see {@link org.apache.avro.ipc.SocketTransceiver}.
 * 
 * Convert List<ByteBuffer> returned by Avro Responder to ChannelBuffer.
 * 
 * @author why
 * @version $Date:2010-06-20 $
 *
 */
public class NettyFrameEncoder extends OneToOneEncoder {

	final static ByteBuffer TERMINATOR = ByteBuffer.allocate(4);
	static {
		TERMINATOR.putInt(0);
		TERMINATOR.flip();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected Object encode(ChannelHandlerContext ctx, Channel channel,
			Object msg) throws Exception {

		List<ByteBuffer> origs = (List<ByteBuffer>)msg;
		List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(origs.size()*2 + 1);
		for (ByteBuffer b : origs) {
			bbs.add(getLengthHeader(b)); // prepend length field
			bbs.add(b);
		}
		bbs.add(TERMINATOR);
		
		return ChannelBuffers.wrappedBuffer(bbs.toArray(new ByteBuffer[bbs.size()]));
	}

	private ByteBuffer getLengthHeader(ByteBuffer buf) {
		ByteBuffer header = ByteBuffer.allocate(4);
		header.putInt(buf.limit());
		header.flip();
		return header;
	}
}
