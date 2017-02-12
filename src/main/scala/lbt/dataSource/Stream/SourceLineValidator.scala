package lbt.dataSource.Stream

import com.typesafe.scalalogging.StrictLogging

object SourceLineValidator extends StrictLogging {

  def apply(sourceLineString: String): SourceLine = {
    val split = splitLine(sourceLineString)
    checkArrayCorrectLength(split)
    SourceLine(split(1).toUpperCase, getDirection(split(2).toInt), split(0), split(3), split(4), split(5).toLong)
  }


  def splitLine(line: String) = line
    .substring(1,line.length-1) // remove leading and trailing square brackets,
    .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    .map(_.replaceAll("\"","")) //takes out double quotations after split
    .map(_.trim) // remove trailing or leading white space
     .tail // discards the first element (always '1')

  def checkArrayCorrectLength(array: Array[String]) = {
    if (array.length != 6) {
      throw new IllegalArgumentException(s"Source array has incorrect number of elements (${array.length}. 6 expected. Or invalid web page retrieved \n " + array)
    }
  }

  def getDirection(integerDirection: Int) = {
    if (integerDirection == 1) "outbound"
    else if (integerDirection == 2) "inbound"
    else throw new IllegalArgumentException("Unknown Direction")
  }
}