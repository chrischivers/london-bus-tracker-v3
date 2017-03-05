package lbt.datasource.streaming

import com.typesafe.scalalogging.StrictLogging
import lbt.datasource.SourceLine

object SourceLineValidator extends StrictLogging {

  def apply(sourceLineString: String): Option[SourceLine] = {
    val split = splitLine(sourceLineString)
    if (arrayCorrectLength(split)) Some(SourceLine(split(1).toUpperCase, split(2).toInt, split(0), split(3), split(4), split(5).toLong))
    else None
  }


  def splitLine(line: String) = line
    .substring(1,line.length-1) // remove leading and trailing square brackets,
    .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    .map(_.replaceAll("\"","")) //takes out double quotations after split
    .map(_.trim) // remove trailing or leading white space
     .tail // discards the first element (always '1')

  def arrayCorrectLength(array: Array[String]): Boolean = {
    if (array.length != 6)  true
    else {
      logger.info(s"Source array has incorrect number of elements (${array.length}. 6 expected. Or invalid web page retrieved \n " + array)
      false
    }
  }
}