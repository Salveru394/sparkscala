import org.apache.spark.SparkContext

object second{
  def main(args:Array[String]):Unit ={

    val value = -15
    if (value < -10 || value >10)
       println("value is out of range")
  else{
    println("value is within the range")
  }
}}
