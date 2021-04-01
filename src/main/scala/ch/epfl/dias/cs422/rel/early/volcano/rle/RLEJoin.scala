package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  private val leftKeys = getLeftKeys
  private var it = Iterator.empty[RLEentry]
  private var mapRight = Map.empty[Tuple, IndexedSeq[RLEentry]]
  private var index = 0L

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    index = 0L

    val rightKeys = getRightKeys
    var loadedRight = IndexedSeq.empty[RLEentry]
    var next = right.next()
    while(next != NilRLEentry) {
      loadedRight = loadedRight :+ next.get
      next = right.next()
    }
    mapRight = loadedRight.groupBy(t => rightKeys.map(t.value(_)))
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (it.hasNext) {
      val entry = it.next()
      index = index + entry.length
      Some(entry)
    } else {
      left.next() match {
        case NilRLEentry => NilRLEentry
        case Some(t) =>
          it = iterate(t)
          next()
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    it = Iterator.empty[RLEentry]
  }

  private def iterate(entry: RLEentry): Iterator[RLEentry] = {
    mapRight.get(leftKeys.map(entry.value(_))) match {
      case Some(entries) =>
        (for (e <- entries)
          yield (RLEentry(
            index,
            entry.length * e.length,
            entry.value :++ e.value
          ))).iterator
      case _ => Iterator.empty
    }
  }
}
