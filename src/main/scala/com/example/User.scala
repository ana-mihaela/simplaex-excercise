package com.example

case class User(data1: String, data2: String, data3: Float, data4: Long, data5: Long)

object User {
  def apply(fields: Array[String]): User = {
    this(fields(0), fields(1), fields(2).toFloat, fields(3).toLong, fields(4).toLong)
  }}
