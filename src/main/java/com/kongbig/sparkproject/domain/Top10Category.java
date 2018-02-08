package com.kongbig.sparkproject.domain;

/**
 * Describe: top10品类
 * Author:   kongbig
 * Data:     2018/2/8.
 */
public class Top10Category {

    private long taskId;
    private long categoryId;
    private long clickCount;
    private long orderCount;
    private long payCount;

    public Top10Category() {
    }

    public Top10Category(long taskId, long categoryId, long clickCount, long orderCount, long payCount) {
        this.taskId = taskId;
        this.categoryId = categoryId;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
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

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

}
