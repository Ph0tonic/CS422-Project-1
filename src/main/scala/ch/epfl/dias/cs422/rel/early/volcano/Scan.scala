package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{
  Elem,
  NilTuple,
  RLEColumn,
  Tuple
}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * A [[Store]] is an in-memory storage the data.
    *
    * Accessing the data is store-type specific and thus
    * you have to convert it to one of the subtypes.
    * See [[getRow]] for an example.
    */
  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )
  private var index = -1
  private var columns = Seq.empty[RLEColumn]

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    index = -1
    scannable match {
      case rleStore: RLEStore =>
        columns =
          (0 until getRowType.getFieldCount).map(i => rleStore.getRLEColumn(i))
      case _ =>
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    index = index + 1
    if (index >= scannable.getRowCount) {
      return NilTuple
    }
    scannable match {
      case rowStore: RowStore => Some(rowStore.getRow(index))
      case _: RLEStore => {
        columns =
          columns.map(c => c.filter(e => e.endVID >= index))
        Some(
          columns.foldLeft(IndexedSeq.empty[Elem])((acc, col) =>
            acc :++ col(0).value
          )
        )
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {}

}
