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

    val mapRight: Map[Tuple, IndexedSeq[Tuple]] =
      right.execute().transpose
        .filter(_.last.asInstanceOf[Boolean])
        .groupBy(t => rightKeys.map(t(_)))

    left
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.dropRight(1))
      .flatMap(t => {
        mapRight.get(leftKeys.map(t(_))) match {
          case Some(tuples) => tuples.map(t :++ _)
          case _            => IndexedSeq.empty
        }
      })
      .transpose
      .map(toHomogeneousColumn)
  }
}
