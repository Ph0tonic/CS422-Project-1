package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
) extends skeleton.Reconstruct[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
    ](left, right)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  private var leftEntry: Option[RLEentry] = NilRLEentry
  private var rightEntry: Option[RLEentry] = NilRLEentry
  private var index: Long = 0

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] = {
    if (rightEntry.isEmpty) {
      rightEntry = right.next()
      if (rightEntry.isEmpty) {
        return NilRLEentry
      }
    }
    if (leftEntry.isEmpty) {
      leftEntry = left.next()
      if (leftEntry.isEmpty) {
        return NilRLEentry
      }
    }

    leftEntry match {
      case Some(l) =>
        rightEntry match {
          case Some(r) =>
            if (l.startVID <= r.startVID && l.endVID >= r.startVID) {
              val next_entry = Some(
                RLEentry(
                  index,
                  l.endVID - r.startVID + 1,
                  l.value ++ r.value
                )
              )
              index = index + 1
              if (r.endVID > l.endVID) {
                leftEntry = NilRLEentry
              } else {
                rightEntry = NilRLEentry
              }
              next_entry
            } else if (l.startVID >= r.startVID && l.startVID <= r.endVID) {
              val next_entry = Some(
                RLEentry(
                  index,
                  r.endVID - l.startVID + 1,
                  l.value ++ r.value
                )
              )
              index = index + 1
              if (r.endVID > l.endVID) {
                leftEntry = NilRLEentry
              } else {
                rightEntry = NilRLEentry
              }

              next_entry
            } else if (l.endVID < r.startVID) {
              leftEntry = NilRLEentry
              next()
            } else {
              rightEntry = NilRLEentry
              next()
            }
        }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    right.close()
    left.close()
  }
}
