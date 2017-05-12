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

import com.weibo.api.motan.codec.Codec;
import com.weibo.api.motan.common.MotanConstants;
import com.weibo.api.motan.rpc.DefaultResponse;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.util.ByteUtil;
import com.weibo.api.motan.util.LoggerUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

/**
 * @author maijunsheng
 * @version 创建时间：2013-5-31
 * 
 */
public class NettyEncoder extends MessageToByteEncoder<Object> {
	private Codec codec;
	private com.weibo.api.motan.transport.Channel client;

	public NettyEncoder(Codec codec, com.weibo.api.motan.transport.Channel client) {
		this.codec = codec;
		this.client = client;
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
		
		long requestId = getRequestId(msg);
		byte[] data = null;
		
		if (msg instanceof Response) {
			try {
				data = codec.encode(client, msg);
			} catch (Exception e) {
				LoggerUtil.error("NettyEncoder encode error, identity=" + client.getUrl().getIdentity(), e);
				Response response = buildExceptionResponse(requestId, e);
				data = codec.encode(client, response);
			}
		} else {
			data = codec.encode(client, msg);
		}

		byte[] transportHeader = new byte[MotanConstants.NETTY_HEADER];
		ByteUtil.short2bytes(MotanConstants.NETTY_MAGIC_TYPE, transportHeader, 0);
		transportHeader[3] = getType(msg);
		ByteUtil.long2bytes(getRequestId(msg), transportHeader, 4);
		ByteUtil.int2bytes(data.length, transportHeader, 12);

		out.writeBytes(transportHeader).writeBytes(data);
	}

	private long getRequestId(Object message) {
		if (message instanceof Request) {
			return ((Request) message).getRequestId();
		} else if (message instanceof Response) {
			return ((Response) message).getRequestId();
		} else {
			return 0;
		}
	}

	private byte getType(Object message) {
		if (message instanceof Request) {
			return MotanConstants.FLAG_REQUEST;
		} else if (message instanceof Response) {
			return MotanConstants.FLAG_RESPONSE;
		} else {
			return MotanConstants.FLAG_OTHER;
		}
	}
	
	private Response buildExceptionResponse(long requestId, Exception e) {
		DefaultResponse response = new DefaultResponse();
		response.setRequestId(requestId);
		response.setException(e);
		return response;
	}
}
