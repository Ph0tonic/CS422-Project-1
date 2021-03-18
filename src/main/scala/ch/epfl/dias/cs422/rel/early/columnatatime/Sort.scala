package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val ordering: Ordering[Tuple] =
      if (collation.getFieldCollations.size() > 0) {
        collation.getFieldCollations
          .toArray(
            Array.ofDim[RelFieldCollation](collation.getFieldCollations.size)
          )
          .map(c => {
            val order = Ordering.by[Tuple, Comparable[Elem]](
              _(c.getFieldIndex).asInstanceOf[Comparable[Elem]]
            )
            if (c.direction.isDescending) {
              order.reverse
            } else {
              order
            }
          })
          .reduce(_.orElse(_))
      } else {
        Ordering.fromLessThan((_, _) => false)
      }

    input
      .execute()
      .transpose
      .filter(_.last.asInstanceOf[Boolean])
      .map(_.toIndexedSeq)
      .sorted(ordering)
      .slice(
        offset.getOrElse(0),
        offset.getOrElse(0) + fetch.getOrElse(Int.MaxValue)
      )
      .transpose
      .map(toHomogeneousColumn)
  }
}
