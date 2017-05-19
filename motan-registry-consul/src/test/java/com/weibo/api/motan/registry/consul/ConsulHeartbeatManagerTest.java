package com.weibo.api.motan.registry.consul;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @Description ConsulHeartbeatManagerTest
 * @author zhanglei28
 * @date 2016年3月22日
 *
 */
public class ConsulHeartbeatManagerTest {
    private ConsulHeartbeatManager heartbeatManager;
    private MockConsulClient client;
    private int heartBeatPeriod = 200;

    @Before
    public void setUp() throws Exception {
        client = new MockConsulClient("localhost", 8500);
        heartbeatManager = new ConsulHeartbeatManager(client);
    }

    @After
    public void tearDown() throws Exception {
        heartbeatManager = null;
    }

    @Test
    public void testStart() throws InterruptedException {
        heartbeatManager.start();
        Map<String, Long> mockServices = new HashMap<String, Long>();
        int serviceNum = 5;

        for (int i = 0; i < serviceNum; i++) {
            String serviceid = "service" + i;
            mockServices.put(serviceid, 0L);
            heartbeatManager.addHeartbeatServcieId(serviceid);
        }

        // 打开心跳
        setHeartbeatSwitcher(true);
        checkHeartbeat(mockServices, true, 1);

        // 关闭心跳
        setHeartbeatSwitcher(false);
        Thread.sleep(100);
        checkHeartbeat(mockServices, false, 1);

    }

    private void checkHeartbeat(Map<String, Long> services, boolean start, int times) throws InterruptedException {
        // 检查times次心跳
        for (int i = 0; i < times; i++) {
            Thread.sleep(ConsulConstants.SWITCHER_CHECK_CIRCLE * 2);
            for (Entry<String, Long> entry : services.entrySet()) {
                long heartbeatTimes = client.getCheckPassTimes(entry.getKey());
                long lastHeartbeatTimes = services.get(entry.getKey());
                services.put(entry.getKey(), heartbeatTimes);
                if (start) { // 心跳打开状态，心跳请求次数应该增加
                    assertTrue(heartbeatTimes > lastHeartbeatTimes);
                } else {// 心跳关闭时，心跳请求次数不应该在改变。
                    assertTrue(heartbeatTimes == lastHeartbeatTimes);
                }
            }
        }
    }

    public void setHeartbeatSwitcher(boolean value) {
        heartbeatManager.setHeartbeatOpen(value);
    }
}
