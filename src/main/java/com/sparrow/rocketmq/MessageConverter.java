package com.sparrow.rocketmq;

import com.sparrow.mq.MQEvent;
import org.apache.rocketmq.common.message.Message;

import java.io.UnsupportedEncodingException;

/**
 * Created by harry on 2017/6/14.
 */
public interface MessageConverter {
    /**
     * 生成mq event
     *
     * @param message
     * @return
     */
    MQEvent fromMessage(Message message) throws UnsupportedEncodingException;

    /**
     * 获取rocket mq messages
     *
     * @param topic
     * @param tag
     * @param event
     * @return
     */
    Message createMessage(String topic, String tag, MQEvent event);
}
