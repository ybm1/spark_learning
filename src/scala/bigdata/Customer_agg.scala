package scala.bigdata
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile

object Customer_agg {
  def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column = {
    val expr = new ApproximatePercentile(
      col.expr,  percentage.expr, accuracy.expr
    ).toAggregateExpression
    new Column(expr)
  }
  def percentile_approx(col: Column, percentage: Column): Column = percentile_approx(
    col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  )
}

