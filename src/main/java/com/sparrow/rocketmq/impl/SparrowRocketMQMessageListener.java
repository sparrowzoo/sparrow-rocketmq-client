/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.rocketmq.impl;

import com.sparrow.cache.CacheClient;
import com.sparrow.constant.cache.KEY;
import com.sparrow.core.spi.JsonFactory;
import com.sparrow.exception.CacheConnectionException;
import com.sparrow.mq.MQContainerProvider;
import com.sparrow.mq.MQEvent;
import com.sparrow.mq.MQHandler;
import com.sparrow.mq.MQ_CLIENT;
import com.sparrow.mq.QueueHandlerMappingContainer;
import com.sparrow.rocketmq.MessageConverter;
import com.sparrow.support.latch.DistributedCountDownLatch;
import com.sparrow.support.redis.impl.RedisDistributedCountDownLatch;
import com.sparrow.utility.StringUtility;
import java.util.List;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by harry on 2017/6/14.
 */
public class SparrowRocketMQMessageListener implements MessageListenerConcurrently {

    public SparrowRocketMQMessageListener() {
        System.out.println("init spring rocket mq message listener");
    }

    private static Logger logger = LoggerFactory.getLogger(SparrowRocketMQMessageListener.class);

    private QueueHandlerMappingContainer queueHandlerMappingContainer = MQContainerProvider.getContainer();
    private MessageConverter messageConverter;
    private DistributedCountDownLatch distributedCountDownLatch;

    public void setDistributedCountDownLatch(DistributedCountDownLatch distributedCountDownLatch) {
        this.distributedCountDownLatch = distributedCountDownLatch;
    }

    public void setQueueHandlerMappingContainer(QueueHandlerMappingContainer queueHandlerMappingContainer) {
        this.queueHandlerMappingContainer = queueHandlerMappingContainer;
    }

    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    protected boolean before(MQEvent event, KEY monitor, String keys) {
        if (distributedCountDownLatch == null) {
            return true;
        }
        logger.info("starting sparrow consume {},monitor {}, keys {}...", JsonFactory.getProvider().toString(event), monitor == null ? "null" : monitor.key(), keys);
        return monitor == null || !distributedCountDownLatch.exist(monitor, keys);
    }

    protected void after(MQEvent event, KEY monitor, String keys) {
        if (distributedCountDownLatch == null) {
            return;
        }
        logger.info("ending sparrow consume {},monitor {},keys {} ...", JsonFactory.getProvider().toString(event), monitor==null?"null":monitor.key(), keys);
        if (StringUtility.isNullOrEmpty(monitor)) {
            return;
        }
        distributedCountDownLatch.consume(monitor,keys);
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
        MessageExt message = list.get(0);
        String type = message.getProperties().get(MQ_CLIENT.CLASS_NAME);
        try {
            if (logger.isInfoEnabled()) {
                logger.info("receive msg:" + message.toString());
            }
            MQHandler handler = queueHandlerMappingContainer.get(type);
            if (handler == null) {
                logger.warn("handler of this type [{}] not found",type);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
            try {
                MQEvent event = messageConverter.fromMessage(message);
                KEY monitor = KEY.parse(message.getProperties().get(MQ_CLIENT.MONITOR_KEY));
                if (!this.before(event, monitor, message.getKeys())) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                handler.handle(event);
                this.after(event, monitor, message.getKeys());
            } catch (Throwable e) {
                logger.error("message error", e);
            }
        } catch (Throwable e) {
            logger.error("process failed, msg : " + message, e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
