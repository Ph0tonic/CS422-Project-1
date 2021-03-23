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
    val filtered = input
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
      val keyIndices = groupSet.toArray.toIndexedSeq

      // Group based on the key produced by the indices in groupSet
      filtered
        .map(_.toIndexedSeq)
        .groupBy(tuple => keyIndices.map(i => tuple(i)))
        .map {
          case (key, tuples) =>
            (key ++
              aggCalls.map(agg =>
                tuples.map(t => agg.getArgument(t)).reduce(aggReduce(_, _, agg))
              )
            ) :+ true
        }
        .toIndexedSeq
        .transpose
        .map(toHomogeneousColumn)
    }
  }
}
