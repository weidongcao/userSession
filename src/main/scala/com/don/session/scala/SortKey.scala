package com.don.session.scala

/**
  * Created by Cao Wei Dong on 2017-03-26.
  */
class SortKey(val clickCount: Int,
              val orderCount: Int,
              val payCount: Int) extends Ordered[SortKey] with Serializable{

  override def compare(that: SortKey): Int = {
    if (clickCount - that.clickCount != 0) {
      clickCount - that.clickCount
    }else if (orderCount - that.orderCount != 0) {
      orderCount - that.orderCount
    }else if (payCount - that.payCount != 0) {
      payCount - that.payCount
    } else{
      0
    }
  }
}
