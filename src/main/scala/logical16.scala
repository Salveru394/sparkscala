object logical16 {
def main(args: Array[String]): Unit = {

    var start = 382
    var end = 582

    var sum = 0
    for (num <- start to end) {
      if (num % 2 == 0) {
        sum += num
      }
    }

    println(sum)
  }
}
