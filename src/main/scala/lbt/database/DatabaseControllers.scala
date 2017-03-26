package lbt.database

import java.util.concurrent.atomic.AtomicLong

/*
 * Database Collection Objects
 */
trait DatabaseControllers {

  val numberInsertsRequested: AtomicLong = new AtomicLong(0)
  val numberInsertsFailed: AtomicLong = new AtomicLong(0)
  val numberInsertsCompleted: AtomicLong = new AtomicLong(0)
  val numberGetsRequested: AtomicLong = new AtomicLong(0)
  val numberDeletesRequested: AtomicLong = new AtomicLong(0)

}


