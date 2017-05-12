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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;


import com.weibo.api.motan.codec.Codec;
import com.weibo.api.motan.common.MotanConstants;
import com.weibo.api.motan.exception.MotanFrameworkException;
import com.weibo.api.motan.exception.MotanServiceException;
import com.weibo.api.motan.rpc.DefaultResponse;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.util.LoggerUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * netty client decode
 * 
 * @author maijunsheng
 * @version 创建时间：2013-5-31
 * 
 */
public class NettyDecoder extends ByteToMessageDecoder {

	private Codec codec;
	private com.weibo.api.motan.transport.Channel client;
	private int maxContentLength = 0;

	public NettyDecoder(Codec codec, com.weibo.api.motan.transport.Channel client, int maxContentLength) {
		this.codec = codec;
		this.client = client;
		this.maxContentLength = maxContentLength;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

		if (in.readableBytes() <= MotanConstants.NETTY_HEADER) {
			return ;
		}

		in.markReaderIndex();

		short type = in.readShort();
		
		if (type != MotanConstants.NETTY_MAGIC_TYPE) {
			in.resetReaderIndex();
			throw new MotanFrameworkException("NettyDecoder transport header not support, type: " + type);
		}

		byte messageType = (byte) in.readShort();
		long requestId = in.readLong();

		int dataLength = in.readInt();

		// FIXME 如果dataLength过大，可能导致问题
		if (in.readableBytes() < dataLength) {
			in.resetReaderIndex();
			return ;
		}

		if (maxContentLength > 0 && dataLength > maxContentLength) {
			LoggerUtil.warn(
					"NettyDecoder transport data content length over of limit, size: {}  > {}. remote={} local={}",
					dataLength, maxContentLength, ctx.channel().remoteAddress(), ctx.channel()
							.localAddress());
			Exception e = new MotanServiceException("NettyDecoder transport data content length over of limit, size: "
					+ dataLength + " > " + maxContentLength);

			if (messageType == MotanConstants.FLAG_REQUEST) {
				Response response = buildExceptionResponse(requestId, e);
				ctx.channel().writeAndFlush(response);
				throw e;
			} else {
				throw e;
			}
		}

		
		byte[] data = new byte[dataLength];

		in.readBytes(data);

		try {
		    String remoteIp = getRemoteIp(ctx.channel());
			out.add(codec.decode(client, remoteIp, data));
		} catch (Exception e) {
			//如果是请求解码失败，直接应答解码异常，如果是应答解码失败，放回处理队列
			if (messageType == MotanConstants.FLAG_REQUEST) {
				Response response = buildExceptionResponse(requestId, e);
				ctx.channel().writeAndFlush(response);
				return ;
			} else {
				out.add(buildExceptionResponse(requestId, e));
				return ;
			}
		}
	}

	private Response buildExceptionResponse(long requestId, Exception e) {
		DefaultResponse response = new DefaultResponse();
		response.setRequestId(requestId);
		response.setException(e);
		return response;
	}
	
	
    private String getRemoteIp(Channel channel) {
        String ip = "";
        SocketAddress remote = channel.remoteAddress();
        if (remote != null) {
            try {
                ip = ((InetSocketAddress) remote).getAddress().getHostAddress();
            } catch (Exception e) {
                LoggerUtil.warn("get remoteIp error!dedault will use. msg:" + e.getMessage() + ", remote:" + remote.toString());
            }
        }
        return ip;

    }
}
