package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val rightKeys = getRightKeys
    val leftKeys = getLeftKeys

    val mapRight: Map[Tuple, Vector[Tuple]] =
      right
        .execute()
        .transpose
        .filter(_.last.asInstanceOf[Boolean])
        .map(_.toIndexedSeq)
        .foldLeft(Map.empty[Tuple, Vector[Tuple]])((acc, t) => {
          val key = rightKeys.map(t(_))
          val tuples = acc.getOrElse(key, Vector.empty[Tuple])
          acc + (key -> (tuples :+ t))
        })

    val t = left
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1).toIndexedSeq)
      .flatMap(t => {
        mapRight.get(leftKeys.map(t(_))) match {
          case Some(tuples) => tuples.map(t :++ _)
          case _            => IndexedSeq.empty
        }
      })
      .transpose
      .map(toHomogeneousColumn)
    t
  }
}
