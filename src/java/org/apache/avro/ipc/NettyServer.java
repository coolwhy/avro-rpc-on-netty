package org.apache.avro.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * Avro Server based on Netty
 * 
 * @author why
 * @version $Date:2010-06-20 $
 */
public class NettyServer extends Thread implements Server {
    private static final Logger logger = Logger.getLogger(
    		NettyServer.class.getName());
    
	private Responder responder;
	private InetSocketAddress addr;

	private Channel serverChannel;
	private ChannelGroup allChannels = 
    	new DefaultChannelGroup("avro-netty-server");
	private ChannelFactory channelFactory;
	
	public NettyServer(Responder responder, InetSocketAddress addr) {
		this.responder = responder;
		this.addr = addr;
		
		setName("AvroNettyServer on " + addr);
		setDaemon(true);
		start();
	}
	
	@Override
	public void close() {
		ChannelGroupFuture future = allChannels.close();
		future.awaitUninterruptibly();
		channelFactory.releaseExternalResources();
	}

	@Override
	public int getPort() {
		return ((InetSocketAddress)serverChannel.getLocalAddress()).getPort();
	}

	@Override
	public void run() {
		channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        	@Override
    		public ChannelPipeline getPipeline() throws Exception {
    			ChannelPipeline p = Channels.pipeline();
    			p.addLast("frameDecoder", new NettyFrameDecoder());
    			p.addLast("frameEncoder", new NettyFrameEncoder());
    			p.addLast("handler", new NettyServerAvroHandler());
    			return p;
    		}
        });
        serverChannel = bootstrap.bind(addr);
        allChannels.add(serverChannel);
	}
	
	class NettyServerAvroHandler extends SimpleChannelUpstreamHandler {
	    
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
	    	allChannels.add(e.getChannel());
			super.channelOpen(ctx, e);
		}

		@SuppressWarnings("unchecked")
		@Override
	    public void messageReceived(
	            ChannelHandlerContext ctx, MessageEvent e) {
	        try {
				List<ByteBuffer> req = (List<ByteBuffer>) e.getMessage();
				List<ByteBuffer> res = responder.respond(req);
				e.getChannel().write(res);
			} catch (IOException ex) {
				logger.warning("unexpect error");
			} finally {
				e.getChannel().close();
			}
	    }

	    @Override
	    public void exceptionCaught(
	            ChannelHandlerContext ctx, ExceptionEvent e) {
	        logger.log(
	                Level.WARNING,
	                "Unexpected exception from downstream.",
	                e.getCause());
	        e.getChannel().close();
	    }
	    
	}

}
