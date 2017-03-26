package com.don.session.spark;

import scala.Serializable;
import scala.math.Ordered;

/**
 * 品类二次排序Key
 * Created by Cao Wei Dong on 2017-03-26.
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

    private long clickCount;
    private long orderCount;
    private long payCount;

    @Override
    public int compare(CategorySortKey other) {
        if (clickCount - other.getClickCount() != 0) {
            return (int) (clickCount - other.getClickCount());
        } else if (orderCount - other.getOrderCount() != 0) {
            return (int) (orderCount - other.orderCount);
        } else if (payCount - other.getPayCount() != 0) {
            return (int) (payCount - other.getPayCount());
        }
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if (clickCount < that.getClickCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount < that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount < that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if (clickCount > that.getClickCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                        && orderCount > that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount > that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if ($less(other)) {
            return true;
        } else if (clickCount == other.getClickCount()
                && orderCount == other.getOrderCount()
                && payCount == other.getPayCount()) {
            return true;
        }
        return false;    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if ($greater(that)) {
            return true;
        } else if (clickCount == that.getClickCount()
                && orderCount == that.getOrderCount()
                && payCount == that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        return compare(other);
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

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }
}
