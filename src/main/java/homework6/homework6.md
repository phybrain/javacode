### 6.javacode/src/main/java/homework6, spark sql
####1.为Spark SQL添加一条自定义命令<br>
  • SHOW VERSION;<br>
  • 显示当前Spark版本和Java版本<br>
   新建command:<br>
   ```scala
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType


case class ShowVersionCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val javaVersion = System.getProperty("java.version");
    val sparkVersion = sparkSession.sparkContext.version
    val version = "java:" + javaVersion + " spark:" + sparkVersion
    Seq(Row(version))
  }
}
   ```
<br>
SparkSqlParser.scala添加代码<br>

   ```scala
override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
  }
   ```

<br>
SqlBase.g4 文件添加：<br>

```scala

statement
    | SHOW VERSION 

ansiNonReserved
    | VERSION

nonReserved
    | VERSION
VERSION: 'VERSION' 
```
####2.构建SQL满足如下要求

通过set spark.sql.planChangeLog.level=WARN;查看
1. 构建一条SQL，同时apply下面三条优化规则：
CombineFilters CollapseProject BooleanSimplification

2. 构建一条SQL，同时apply下面五条优化规则：
ConstantFolding PushDownPredicates ReplaceDistinctWithAggregate ReplaceExceptWithAntiJoin FoldablePropagation