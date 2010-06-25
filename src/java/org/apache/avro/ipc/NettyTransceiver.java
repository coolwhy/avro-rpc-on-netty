package org.apache.avro.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * Avro Transceiver based on Netty
 * 
 * @author why
 * @version $Date:2010-06-20 $
 */
public class NettyTransceiver extends Transceiver {
    private static final Logger logger = Logger.getLogger(
    		NettyTransceiver.class.getName());
    
    private ChannelFactory channelFactory;
    private Channel channel;
	private BlockingQueue<List<ByteBuffer>> answers = 
		new LinkedBlockingQueue<List<ByteBuffer>>();
	private AtomicInteger requestCount = new AtomicInteger(0);
	
	// an identification which means an network exception occurred while waiting for the answer
	private static final List<ByteBuffer> EXCEPTION_OCCURRED = 
		new ArrayList<ByteBuffer>(0);
	
	public NettyTransceiver(InetSocketAddress addr) {
        // Set up.
		channelFactory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);

        // Configure the event pipeline factory.
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
    		@Override
    		public ChannelPipeline getPipeline() throws Exception {
    			ChannelPipeline p = Channels.pipeline();
    			p.addLast("frameDecoder", new NettyFrameDecoder());
    			p.addLast("frameEncoder", new NettyFrameEncoder());
    			p.addLast("handler", new NettyClientAvroHandler());
    			return p;
    		}
        });

        bootstrap.setOption("tcpNoDelay", true);
        
        // Make a new connection.
        ChannelFuture channelFuture = bootstrap.connect(addr);
        channelFuture.awaitUninterruptibly();
        if (!channelFuture.isSuccess()) {
        	channelFuture.getCause().printStackTrace();
        	throw new RuntimeException(channelFuture.getCause());
        }
        channel = channelFuture.getChannel();
	}

	public void close() {
		// Close the connection.
		channel.close().awaitUninterruptibly();
		// Shut down all thread pools to exit.
		channelFactory.releaseExternalResources();
	}
	
	@Override
	public String getRemoteName() {
		return channel.getRemoteAddress().toString();
	}

	@Override
	public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
		// asynchronous operation
		channel.write(buffers);
		requestCount.incrementAndGet();
	}
	
	@Override
	public List<ByteBuffer> readBuffers() throws IOException {
		// block the operation until the answer is got
		boolean interrupted = false;
		List<ByteBuffer> res;
		while(true) {
			try {
				res = answers.take();
				break;
			} catch (InterruptedException e) {
				interrupted = true;
			}
		}
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (res==EXCEPTION_OCCURRED) {
        	throw new IOException("wait for response failed");
        }
        return res;
	}

	class NettyClientAvroHandler extends SimpleChannelUpstreamHandler {
		//private Channel channel; // connection
		
	    @Override
	    public void handleUpstream(
	            ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
	        if (e instanceof ChannelStateEvent) {
	            logger.info(e.toString());
	        }
	        super.handleUpstream(ctx, e);
	    }

	    @Override
	    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
	            throws Exception {
	        //channel = e.getChannel();
	        super.channelOpen(ctx, e);
	    }

	    @SuppressWarnings("unchecked")
		@Override
	    public void messageReceived(
	            ChannelHandlerContext ctx, final MessageEvent e) {
	    	requestCount.decrementAndGet();
	    	answers.offer((List<ByteBuffer>)e.getMessage());
	    }

	    @Override
	    public void exceptionCaught(
	            ChannelHandlerContext ctx, ExceptionEvent e) {
	        logger.log(
	                Level.WARNING,
	                "Unexpected exception from downstream.",
	                e.getCause());
	        e.getChannel().close();
	        int leftRequest = requestCount.get();
	        for(int i=0;i<leftRequest;i++) {
	        	// let the blocking waiting exit
	        	answers.offer(EXCEPTION_OCCURRED);
	        }
	    }
	    
	}

}
