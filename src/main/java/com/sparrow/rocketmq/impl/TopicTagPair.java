package com.sparrow.rocketmq.impl;

import java.util.List;

/**
 * Created by harry on 2017/6/14.
 */
public class TopicTagPair {
    private String topic;
    private List<String> tags;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}
