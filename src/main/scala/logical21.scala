object logical21 {
  def main(args: Array[String]): Unit = {

    for (i <- 10 to -5 by -1){
      println(s"${i}@${i-1}")
    }
  }
}