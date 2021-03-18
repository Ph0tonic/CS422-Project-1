package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    val filtered =
      input
        .execute()
        .transpose
        .filter(_.last.asInstanceOf[Boolean])
    if (filtered.isEmpty && groupSet.isEmpty) {
      IndexedSeq(
        aggCalls
          .map(aggEmptyValue)
          .foldLeft(IndexedSeq.empty[Elem])((a, b) => a :+ b)
          .asInstanceOf[Tuple] :+ true
      ).transpose
        .map(toHomogeneousColumn)
    } else {
      val keyIndices = groupSet.toArray

      // Group based on the key produced by the indices in groupSet
      filtered
        .map(_.toIndexedSeq)
        .foldLeft(Map.empty[Tuple, Vector[Tuple]])((acc, tuple) => {
          val key: Tuple = keyIndices.map(i => tuple(i))
          acc.get(key) match {
            case Some(arr: Vector[Tuple]) => acc + (key -> (arr :+ tuple))
            case _                        => acc + (key -> Vector(tuple))
          }
        })
        .toIndexedSeq
        .map {
          case (key, tuples) =>
            key.++(
              aggCalls.map(agg =>
                tuples.map(t => agg.getArgument(t)).reduce(aggReduce(_, _, agg))
              )
            ) :+ true
        }
        .transpose
        .map(toHomogeneousColumn)
    }
  }
}
