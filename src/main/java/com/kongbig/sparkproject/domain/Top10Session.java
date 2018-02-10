package com.kongbig.sparkproject.domain;

import java.io.Serializable;

/**
 * Describe: top10活跃session
 * Author:   kongbig
 * Data:     2018/2/10.
 */
public class Top10Session implements Serializable {

    private static final long serialVersionUID = 8608408680939611642L;
    
    private long taskId;
    private long categoryId;
    private String sessionId;
    private long clickCount;

    public Top10Session() {}

    public Top10Session(long taskId, long categoryId, String sessionId, long clickCount) {
        this.taskId = taskId;
        this.categoryId = categoryId;
        this.sessionId = sessionId;
        this.clickCount = clickCount;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
    
}
