package com.own.hydropower

import com.own.hydropower.data2dbs.DataFromKafka

object Main {
  def main(args: Array[String]): Unit = {
    val data = new DataFromKafka
    data.getConsumptionData()
  }
}
