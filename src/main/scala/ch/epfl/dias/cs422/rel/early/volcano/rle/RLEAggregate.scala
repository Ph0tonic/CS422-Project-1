package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{
  Elem,
  NilRLEentry,
  NilTuple,
  RLEentry,
  Tuple
}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  protected var aggregated = List.empty[(Tuple, Vector[Tuple])]
  private var index = -1

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    index = -1

    input.open()
    var next = input.next()
    if (next == NilRLEentry && groupSet.isEmpty) {
      // return aggEmptyValue for each aggregate.
      aggregated = List(
        IndexedSeq.empty[Elem] -> Vector(
          aggCalls
            .map(aggEmptyValue)
            .foldLeft(IndexedSeq.empty[Elem])((a, b) => a :+ b)
            .asInstanceOf[Tuple]
        )
      )
    } else {
      // Group based on the key produced by the indices in groupSet
      val keyIndices = groupSet.toArray
      var aggregates = Map.empty[Tuple, Vector[Tuple]]
      while (next != NilRLEentry) {
        val entry: RLEentry = next.get
        val tuples : Vector[Tuple] = Vector.range(0,entry.length).map(_=>entry.value)
        val key: Tuple = keyIndices.map(i => entry.value(i))
        aggregates = aggregates.get(key) match {
          case Some(arr: Vector[Tuple]) => aggregates + (key -> (arr :++ tuples))
          case _                        => aggregates + (key -> tuples)
        }
        next = input.next()
      }

      aggregated = aggregates.toList
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    aggregated match {
      case (key, tuples) :: tail =>
        aggregated = tail
        index = index + 1
        Some(
          RLEentry(
            index,
            1,
            key.++(
              aggCalls.map(agg =>
                tuples.map(t => agg.getArgument(t)).reduce(aggReduce(_, _, agg))
              )
            )
          )
        )
      case _ => NilRLEentry
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
    aggregated = List.empty[(Tuple, Vector[Tuple])]
  }
}
