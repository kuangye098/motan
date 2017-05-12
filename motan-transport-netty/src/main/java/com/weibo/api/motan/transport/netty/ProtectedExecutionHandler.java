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


import com.weibo.api.motan.exception.MotanErrorMsgConstant;
import com.weibo.api.motan.exception.MotanServiceException;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.DefaultResponse;
import com.weibo.api.motan.util.LoggerUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.SingleThreadEventExecutor;

/**
 * @author maijunsheng
 * @version 创建时间：2013-6-7
 * 
 */
public class ProtectedExecutionHandler implements RejectedExecutionHandler {


	@Override
	public void rejected(Runnable task, SingleThreadEventExecutor executor) {

	}
}
