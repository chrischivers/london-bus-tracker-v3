package datasource

import lbt.DataSourceConfig
import lbt.datasource.BusDataSource.BusDataSource
import lbt.datasource.streaming.SourceLineValidator

class TestDataSource(config: DataSourceConfig, lineList: Option[List[String]] = None) extends BusDataSource(config) {

  val testLines: List[String] = lineList.getOrElse(List[String](
    "[1,\"490009774E\",\"352\",2,\"Bromley North\",\"YX62DYN\"," + System.currentTimeMillis() + "]",
    "[1,\"490010622BA\",\"74\",2,\"Putney\",\"LX05EYM\"," + System.currentTimeMillis() + "]",
    "[1,\"490012612C\",\"329\",2,\"Enfield\",\"LJ08CTZ\"," + System.currentTimeMillis() + "]",
    "[1,\"490000043CB\",\"31\",1,\"White City\",\"LK04HXU\"," + System.currentTimeMillis() + "]",
    "[1,\"490007220Z\",\"55\",2,\"Bakers Arms\",\"LTZ1463\"," + System.currentTimeMillis() + "]"))

  val testLineIterator = testLines.toIterator

  override def hasNext: Boolean = testLineIterator.hasNext

  override def next() = {
    numberLinesStreamed.incrementAndGet()
    SourceLineValidator(testLineIterator.next())
  }
}
