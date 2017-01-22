package lbt.dataSource

object BusSourceLineFormatter{

  def apply(sourceLineString: String): BusSourceLine = {
    val x = splitLine(sourceLineString)
    checkArrayCorrectLength(x)
    val busRoute = BusRoute(x(1).toUpperCase, getDirection(x(2).toInt))
    //val busStop = BusDefinitions.busRouteDefinitions(busRoute).filter(stop => stop.busStop.busStopID == x(0)).head.busStop
    new BusSourceLine(busRoute, x(0), x(3), x(4), x(5).toLong)
  }


  def splitLine(line: String) = line
    .substring(1,line.length-1) // remove leading and trailing square brackets,
    .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    .map(_.replaceAll("\"","")) //takes out double quotations after split
    .map(_.trim) // remove trailing or leading white space
     .tail // discards the first element (always '1')

  def checkArrayCorrectLength(array: Array[String]) = {
    if (array.length != BusDataSource.fieldVector.length) {
      Logger.debug("Source array has incorrect number of elements. Or invalid web page retrieved \n " + array)
      throw new IllegalArgumentException("Source array has incorrect number of elements. Or invalid web page retrieved \n " + array)
    }
  }

  def getDirection(integerDirection:Int) = {
    if (integerDirection == 1) "outbound"
    else if (integerDirection == 2) "inbound"
    else throw new IllegalArgumentException("Unknown Direction")
  }