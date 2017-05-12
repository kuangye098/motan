/*
 *  Copyright 2009-2016 Weibo, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.weibo.api.motan.transport.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.weibo.api.motan.common.ChannelState;
import com.weibo.api.motan.common.MotanConstants;
import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.core.DefaultThreadFactory;
import com.weibo.api.motan.exception.MotanFrameworkException;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.rpc.URL;
import com.weibo.api.motan.transport.AbstractServer;
import com.weibo.api.motan.transport.MessageHandler;
import com.weibo.api.motan.transport.TransportException;
import com.weibo.api.motan.util.LoggerUtil;
import com.weibo.api.motan.util.StatisticCallback;
import com.weibo.api.motan.util.StatsUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * 
 * <pre>
 * 		netty server 的相关优化
 * 		1） server 的 executor handler 相关防护
 * 		2） server 的 隔离保护，不同方法。
 * 		3） 线程池调优
 * 		4） client 请求drop （提供主动和被动策略）
 * 		5） 增加降级开关。
 * 		6） server 端的超时控制
 * 		7） 关注 OOM的问题
 * 		8） Queue 大小的设置 
 * 		9） server端接收包的大小限制
 * </pre>
 * 
 * @author maijunsheng
 * 
 */
public class NettyServer extends AbstractServer implements StatisticCallback {
	// default io thread is Runtime.getRuntime().availableProcessors() * 2
//	private final static ChannelFactory channelFactory = new NioServerSocketChannelFactory(
//			Executors.newCachedThreadPool(new DefaultThreadFactory("nettyServerBoss", true)),
//			Executors.newCachedThreadPool(new DefaultThreadFactory("nettyServerWorker", true)));

	// 单端口需要对应单executor 1) 为了更好的隔离性 2) 为了防止被动releaseExternalResources:
	private StandardThreadExecutor standardThreadExecutor = null;
	
	protected NettyServerChannelManage channelManage = null;
	private ServerChannel serverChannel;
	private ServerBootstrap bootstrap;
	private MessageHandler messageHandler;

	public NettyServer(URL url, MessageHandler messageHandler) {
		super(url);
		this.messageHandler = messageHandler;
	}

	@Override
	public Response request(Request request) throws TransportException {
		throw new MotanFrameworkException("NettyServer request(Request request) method unsupport: url: " + url);
	}

	@Override
	public synchronized boolean open() {
		if (isAvailable()) {
			LoggerUtil.warn("NettyServer ServerChannel already Open: url=" + url);
			return true;
		}

		LoggerUtil.info("NettyServer ServerChannel start Open: url=" + url);

		initServerBootstrap();

		ChannelFuture channelFuture = bootstrap.bind(url.getPort());

		serverChannel = (ServerChannel)channelFuture.channel();

		state = ChannelState.ALIVE;

		StatsUtil.registryStatisticCallback(this);
		LoggerUtil.info("NettyServer ServerChannel finish Open: url=" + url);

		return state.isAliveState();
	}

	private synchronized void initServerBootstrap() {
		boolean shareChannel = url.getBooleanParameter(URLParamType.shareChannel.getName(),
				URLParamType.shareChannel.getBooleanValue());
		final int maxContentLength = url.getIntParameter(URLParamType.maxContentLength.getName(),
				URLParamType.maxContentLength.getIntValue());
		int maxServerConnection = url.getIntParameter(URLParamType.maxServerConnection.getName(),
				URLParamType.maxServerConnection.getIntValue());
		int workerQueueSize = url.getIntParameter(URLParamType.workerQueueSize.getName(),
				URLParamType.workerQueueSize.getIntValue());

		int minWorkerThread = 0, maxWorkerThread = 0;

		if (shareChannel) {
			minWorkerThread = url.getIntParameter(URLParamType.minWorkerThread.getName(),
					MotanConstants.NETTY_SHARECHANNEL_MIN_WORKDER);
			maxWorkerThread = url.getIntParameter(URLParamType.maxWorkerThread.getName(),
					MotanConstants.NETTY_SHARECHANNEL_MAX_WORKDER);
		} else {
			minWorkerThread = url.getIntParameter(URLParamType.minWorkerThread.getName(),
					MotanConstants.NETTY_NOT_SHARECHANNEL_MIN_WORKDER);
			maxWorkerThread = url.getIntParameter(URLParamType.maxWorkerThread.getName(),
					MotanConstants.NETTY_NOT_SHARECHANNEL_MAX_WORKDER);
		}

		
		standardThreadExecutor = (standardThreadExecutor != null && !standardThreadExecutor.isShutdown()) ? standardThreadExecutor
				: new StandardThreadExecutor(minWorkerThread, maxWorkerThread, workerQueueSize,
						new DefaultThreadFactory("NettyServer-" + url.getServerPortStr(), true));
		standardThreadExecutor.prestartAllCoreThreads();

		// 连接数的管理，进行最大连接数的限制 
		channelManage = new NettyServerChannelManage(maxServerConnection);

		final NettyChannelHandler handler = new NettyChannelHandler(NettyServer.this, messageHandler,
				standardThreadExecutor);

		EventLoopGroup bossGroup = new NioEventLoopGroup(1,new ThreadFactory() {
				private AtomicInteger threadIndex = new AtomicInteger(0);

				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, String.format("NettyBoss_%d", this.threadIndex.incrementAndGet()));
				}
		});

		EventLoopGroup workerGroup = new NioEventLoopGroup();

		bootstrap = new ServerBootstrap();
		bootstrap.group(bossGroup,workerGroup)
				 .channel(NioServerSocketChannel.class)
				 .childOption(ChannelOption.TCP_NODELAY,true)
				 .childOption(ChannelOption.SO_KEEPALIVE,true)
				 .childHandler(new ChannelInitializer<SocketChannel>() {
					 @Override
					 public void initChannel(SocketChannel ch) throws Exception {
						 ch.pipeline().addLast(channelManage);
						 ch.pipeline().addLast(new NettyEncoder(codec, NettyServer.this));

						 ch.pipeline().addLast(new NettyDecoder(codec, NettyServer.this, maxContentLength));
						 ch.pipeline().addLast(handler);
					 }
				 });
	}

	@Override
	public synchronized void close() {
		close(0);
	}

	@Override
	public synchronized void close(int timeout) {
		if (state.isCloseState()) {
			LoggerUtil.info("NettyServer close fail: already close, url={}", url.getUri());
			return;
		}

		if (state.isUnInitState()) {
			LoggerUtil.info("NettyServer close Fail: don't need to close because node is unInit state: url={}",
					url.getUri());
			return;
		}

		try {
			// close listen socket
			serverChannel.close();
			// close all clients's channel
			channelManage.close();
			// shutdown the threadPool
			standardThreadExecutor.shutdownNow();
			// 设置close状态
			state = ChannelState.CLOSE;
			// 取消统计回调的注册
			StatsUtil.unRegistryStatisticCallback(this);
			LoggerUtil.info("NettyServer close Success: url={}", url.getUri());
		} catch (Exception e) {
			LoggerUtil.error("NettyServer close Error: url=" + url.getUri(), e);
		}
	}

	@Override
	public boolean isClosed() {
		return state.isCloseState();
	}

	@Override
	public boolean isAvailable() {
		return state.isAliveState();
	}

	@Override
	public URL getUrl() {
		return url;
	}

	/**
	 * 统计回调接口
	 */
	@Override
	public String statisticCallback() {
		return String.format(
				"identity: %s connectionCount: %s taskCount: %s queueCount: %s maxThreadCount: %s maxTaskCount: %s",
				url.getIdentity(), channelManage.getChannels().size(), standardThreadExecutor.getSubmittedTasksCount(),
				standardThreadExecutor.getQueue().size(), standardThreadExecutor.getMaximumPoolSize(),
				standardThreadExecutor.getMaxSubmittedTaskCount());
	}

	/**
	 * 是否已经绑定端口
	 */
	@Override
	public boolean isBound() {
		return serverChannel != null && serverChannel.isOpen();
	}

	public MessageHandler getMessageHandler() {
		return messageHandler;
	}

	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}
}
