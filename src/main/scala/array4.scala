object array4 {
def main (args:Array[String]): Unit = {

  var double = Array(10.5,20.5,30.5,40.5,50.5)
  var max = double(0)

  for (i <-0 until double.length){
    if (max < double(i)){
      max = double(i)
    }
  }
  println(max)
}
}
