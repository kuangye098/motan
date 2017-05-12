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
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import com.weibo.api.motan.common.ChannelState;
import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.exception.MotanErrorMsgConstant;
import com.weibo.api.motan.exception.MotanFrameworkException;
import com.weibo.api.motan.exception.MotanServiceException;
import com.weibo.api.motan.rpc.Future;
import com.weibo.api.motan.rpc.FutureListener;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.rpc.URL;
import com.weibo.api.motan.transport.TransportException;
import com.weibo.api.motan.util.ExceptionUtil;
import com.weibo.api.motan.util.LoggerUtil;
import com.weibo.api.motan.util.MotanFrameworkUtil;

/**
 * @author zifei
 * @version 创建时间：2017-5-17
 * 
 */
public class NettyChannel implements com.weibo.api.motan.transport.Channel {
	private volatile ChannelState state = ChannelState.UNINIT;

	private NettyClient nettyClient;

	private Channel channel = null;

	private InetSocketAddress remoteAddress = null;
	private InetSocketAddress localAddress = null;

	public NettyChannel(NettyClient nettyClient) {
		this.nettyClient = nettyClient;
		this.remoteAddress = new InetSocketAddress(nettyClient.getUrl().getHost(), nettyClient.getUrl().getPort());
	}

	@Override
	public Response request(Request request) throws TransportException {
	    int timeout = nettyClient.getUrl().getMethodParameter(request.getMethodName(), request.getParamtersDesc(),
	            URLParamType.requestTimeout.getName(), URLParamType.requestTimeout.getIntValue());
		if (timeout <= 0) {
               throw new MotanFrameworkException("NettyClient init Error: timeout(" + timeout + ") <= 0 is forbid.",
                       MotanErrorMsgConstant.FRAMEWORK_INIT_ERROR);
           }
		NettyResponseFuture response = new NettyResponseFuture(request, timeout, this.nettyClient);
		this.nettyClient.registerCallback(request.getRequestId(), response);

		ChannelFuture writeFuture = this.channel.writeAndFlush(request);

		boolean result = writeFuture.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS);

		if (result && writeFuture.isSuccess()) {
			response.addListener(new FutureListener() {
				@Override
				public void operationComplete(Future future) throws Exception {
					if (future.isSuccess() || (future.isDone() && ExceptionUtil.isBizException(future.getException()))) {
						// 成功的调用 
						nettyClient.resetErrorCount();
					} else {
						// 失败的调用 
						nettyClient.incrErrorCount();
					}
				}
			});
			return response;
		}

		writeFuture.cancel(false);
		response = this.nettyClient.removeCallback(request.getRequestId());

		if (response != null) {
			response.cancel();
		}

		// 失败的调用 
		nettyClient.incrErrorCount();

		if (writeFuture.cause() != null) {
			throw new MotanServiceException("NettyChannel send request to server Error: url="
					+ nettyClient.getUrl().getUri() + " local=" + localAddress + " "
					+ MotanFrameworkUtil.toString(request), writeFuture.cause());
		} else {
			throw new MotanServiceException("NettyChannel send request to server Timeout: url="
					+ nettyClient.getUrl().getUri() + " local=" + localAddress + " "
					+ MotanFrameworkUtil.toString(request));
		}
	}

	@Override
	public synchronized boolean open() {
		if (isAvailable()) {
			LoggerUtil.warn("the channel already open, local: " + localAddress + " remote: " + remoteAddress + " url: "
					+ nettyClient.getUrl().getUri());
			return true;
		}

		try {
			ChannelFuture channleFuture = nettyClient.getBootstrap().connect(
					new InetSocketAddress(nettyClient.getUrl().getHost(), nettyClient.getUrl().getPort()));

			long start = System.currentTimeMillis();

			int timeout = nettyClient.getUrl().getIntParameter(URLParamType.connectTimeout.getName(), URLParamType.connectTimeout.getIntValue());
			if (timeout <= 0) {
	            throw new MotanFrameworkException("NettyClient init Error: timeout(" + timeout + ") <= 0 is forbid.",
	                    MotanErrorMsgConstant.FRAMEWORK_INIT_ERROR);
			}
			// 不去依赖于connectTimeout
			boolean result = channleFuture.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS);
            boolean success = channleFuture.isSuccess();

			if (result && success) {
				channel = channleFuture.channel();
				if (channel.localAddress() != null && channel.localAddress() instanceof InetSocketAddress) {
					localAddress = (InetSocketAddress) channel.localAddress();
				}

				state = ChannelState.ALIVE;
				return true;
			}
            boolean connected = false;
            if(channleFuture.channel() != null){
                connected = channleFuture.channel().isActive();
            }

			if (channleFuture.cause() != null) {
				channleFuture.cancel(false);
				throw new MotanServiceException("NettyChannel failed to connect to server, url: "
						+ nettyClient.getUrl().getUri()+ ", result: " + result + ", success: " + success + ", connected: " + connected, channleFuture.cause());
			} else {
				channleFuture.cancel(false);
                throw new MotanServiceException("NettyChannel connect to server timeout url: "
                        + nettyClient.getUrl().getUri() + ", cost: " + (System.currentTimeMillis() - start) + ", result: " + result + ", success: " + success + ", connected: " + connected);
            }
		} catch (MotanServiceException e) {
			throw e;
		} catch (Exception e) {
			throw new MotanServiceException("NettyChannel failed to connect to server, url: "
					+ nettyClient.getUrl().getUri(), e);
		} finally {
			if (!state.isAliveState()) {
				nettyClient.incrErrorCount();
			}
		}
	}

	@Override
	public synchronized void close() {
		close(0);
	}

	@Override
	public synchronized void close(int timeout) {
		try {
			state = ChannelState.CLOSE;

			if (channel != null) {
				channel.close();
			}
		} catch (Exception e) {
			LoggerUtil
					.error("NettyChannel close Error: " + nettyClient.getUrl().getUri() + " local=" + localAddress, e);
		}
	}

	@Override
	public InetSocketAddress getLocalAddress() {
		return localAddress;
	}

	@Override
	public InetSocketAddress getRemoteAddress() {
		return remoteAddress;
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
		return nettyClient.getUrl();
	}
}
