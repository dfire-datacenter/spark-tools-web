package com.dfire.command.utils

import org.apache.spark.util.AccumulatorV2

class SQLMetric(val metricType: String, initValue: Long = 0L) extends AccumulatorV2[Long, Long] {
  // This is a workaround for SPARK-11013.
  // We may use -1 as initial value of the accumulator, if the accumulator is valid, we will
  // update it at the end of task and the value will be at least 0. Then we can filter out the -1
  // values before calculate max, min, etc.
  private[this] var _value = initValue
  private var _zeroValue = initValue

  override def copy(): SQLMetric = {
    val newAcc = new SQLMetric(metricType, _value)
    newAcc._zeroValue = initValue
    newAcc
  }

  override def reset(): Unit = _value = _zeroValue

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o: SQLMetric => _value += o.value
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def isZero(): Boolean = _value == _zeroValue

  override def add(v: Long): Unit = _value += v

  def set(v: Long): Unit = _value = v

  def +=(v: Long): Unit = _value += v

  override def value: Long = _value

}

object SQLMetrics {
  private val SUM_METRIC = "sum"
  private val SIZE_METRIC = "size"
  private val TIMING_METRIC = "timing"
  private val AVERAGE_METRIC = "average"
  private val baseForAvgMetric: Int = 10
}
