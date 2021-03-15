package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{
  NilRLEentry,
  NilTuple,
  RLEentry,
  Tuple
}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Decode[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](input)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var tuple:Option[Tuple] = NilTuple
  private var count: Long = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    count = 0
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (count > 0) {
      count = count - 1
      tuple
    } else {
      val nextTuple: Option[RLEentry] = input.next()
      nextTuple match {
        case NilRLEentry => NilTuple
        case Some(entry) =>
          tuple = Some(entry.value)
          count = entry.length - 1
          tuple
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
    tuple = NilTuple
  }

}
