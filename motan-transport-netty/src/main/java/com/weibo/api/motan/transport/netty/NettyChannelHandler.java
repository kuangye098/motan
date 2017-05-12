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

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;


import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.exception.MotanErrorMsgConstant;
import com.weibo.api.motan.exception.MotanFrameworkException;
import com.weibo.api.motan.exception.MotanServiceException;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.rpc.DefaultResponse;
import com.weibo.api.motan.rpc.RpcContext;
import com.weibo.api.motan.transport.Channel;
import com.weibo.api.motan.transport.MessageHandler;
import com.weibo.api.motan.util.LoggerUtil;
import com.weibo.api.motan.util.NetUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 
 * @author zifei
 * @version 创建时间：2017-5-17
 * 
 */
@ChannelHandler.Sharable
public class NettyChannelHandler extends SimpleChannelInboundHandler<Object> {
	private ThreadPoolExecutor threadPoolExecutor;
	private MessageHandler messageHandler;
	private Channel serverChannel;

	public NettyChannelHandler(Channel serverChannel) {
		this.serverChannel = serverChannel;
	}

	public NettyChannelHandler(Channel serverChannel, MessageHandler messageHandler) {
		this.serverChannel = serverChannel;
		this.messageHandler = messageHandler;
	}

	public NettyChannelHandler(Channel serverChannel, MessageHandler messageHandler,
			ThreadPoolExecutor threadPoolExecutor) {
		this.serverChannel = serverChannel;
		this.messageHandler = messageHandler;
		this.threadPoolExecutor = threadPoolExecutor;
	}

	public void channelConnected(ChannelHandlerContext ctx) throws Exception {
		LoggerUtil.info("NettyChannelHandler channelConnected: remote=" + ctx.channel().remoteAddress()
				+ " local=" + ctx.channel().localAddress() + " cause=" + ctx.voidPromise().cause());
	}

	public void channelDisconnected(ChannelHandlerContext ctx) throws Exception {
		LoggerUtil.info("NettyChannelHandler channelDisconnected: remote=" + ctx.channel().remoteAddress()
				+ " local=" + ctx.channel().localAddress() + " cause=" + ctx.voidPromise().cause());
	}

	@Override
	public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

		if (msg instanceof Request) {
			processRequest(ctx, (Request)msg);
		} else if (msg instanceof Response) {
			processResponse(ctx, (Response)msg);
		} else {
			LoggerUtil.error("NettyChannelHandler messageReceived type not support: class=" + msg.getClass());
			throw new MotanFrameworkException("NettyChannelHandler messageReceived type not support: class="
					+ msg.getClass());
		}
	}

	/**
	 * <pre>
	 *  request process: 主要来自于client的请求，需要使用threadPoolExecutor进行处理，避免service message处理比较慢导致iothread被阻塞
	 * </pre>
	 * 
	 * @param ctx
	 * @param request
	 */
	private void processRequest(final ChannelHandlerContext ctx,final Request request) {

		request.setAttachment(URLParamType.host.getName(), NetUtils.getHostName(ctx.channel().remoteAddress()));

		final long processStartTime = System.currentTimeMillis();

		// 使用线程池方式处理
		try {
			threadPoolExecutor.execute(new Runnable() {
				@Override
                public void run() {
				    try{
				        RpcContext.init(request);
	                    processRequest(ctx, request, processStartTime);
				    }finally{
				        RpcContext.destroy();
				    }
                }
            });
		} catch (RejectedExecutionException rejectException) {
			DefaultResponse response = new DefaultResponse();
			response.setRequestId(request.getRequestId());
			response.setException(new MotanServiceException("process thread pool is full, reject",
					MotanErrorMsgConstant.SERVICE_REJECT));
			response.setProcessTime(System.currentTimeMillis() - processStartTime);
			ctx.channel().writeAndFlush(response);

			LoggerUtil
					.debug("process thread pool is full, reject, active={} poolSize={} corePoolSize={} maxPoolSize={} taskCount={} requestId={}",
							threadPoolExecutor.getActiveCount(), threadPoolExecutor.getPoolSize(),
							threadPoolExecutor.getCorePoolSize(), threadPoolExecutor.getMaximumPoolSize(),
							threadPoolExecutor.getTaskCount(), request.getRequestId());
		}
	}

	private void processRequest(ChannelHandlerContext ctx, Request request, long processStartTime) {
		Object result = messageHandler.handle(serverChannel, request);

		DefaultResponse response = null;

		if (!(result instanceof DefaultResponse)) {
			response = new DefaultResponse(result);
		} else {
			response = (DefaultResponse) result;
		}

		response.setRequestId(request.getRequestId());
		response.setProcessTime(System.currentTimeMillis() - processStartTime);

		if (ctx.channel().isActive()) {
			ctx.channel().writeAndFlush(response);
		}
	}

	private void processResponse(ChannelHandlerContext ctx, Response response) {
		messageHandler.handle(serverChannel,response);
	}

	public void exceptionCaught(ChannelHandlerContext ctx) throws Exception {
		LoggerUtil.error("NettyChannelHandler exceptionCaught: remote=" + ctx.channel().remoteAddress()
				+ " local=" + ctx.channel().localAddress());

		ctx.channel().close();
	}
}
