import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import  org.apache.spark.sql.types.Decimal

case class MultiplyOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions  {
    case Multiply(left,right,error) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.isInstanceOf[Decimal] => //value.asInstanceOf[Decimal] == 1.0
      println("-----------------------------My Optimization------------------------------")
      left

  }
}