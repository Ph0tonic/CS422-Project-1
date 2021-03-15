package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, RLEentry, Tuple}
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
  private var mapRight = Map.empty[Tuple, Vector[RLEentry]]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()

    val rightKeys = getRightKeys

    mapRight =
      right.foldLeft(Map.empty[Tuple, Vector[RLEentry]])((acc, entry) => {
        val key = rightKeys.map(entry.value(_))
        val entries = acc.getOrElse(key, Vector.empty[RLEentry])
        acc + (key -> (entries :+ entry))
      })
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (it.hasNext) {
      Some(it.next())
    } else {
      left.next() match {
        case NilTuple => NilTuple
        case Some(t) =>
          it = iterate(t)
          next()
      }
    }
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = left.close()

  private def iterate(entry: RLEentry): Iterator[RLEentry] = {
    mapRight.get(leftKeys.map(entry.value(_))) match {
      case Some(entries) =>
        (for (e <- entries)
          yield (RLEentry(entry.startVID, entry.length, entry.value :++ e.asInstanceOf[Tuple]))).iterator
      case _ => Iterator.empty
    }
  }
}
