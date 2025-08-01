object FilterNaNExample3 extends App {
  val list2: List[Any] = List(1, Float.NaN, "hello", 3)

  // 使用 collect 方法结合模式匹配筛选出 Float.NaN
  val result = list2.collect {
    case num: Float if num.isNaN => num
  }
  println(result) // 输出: List(NaN)
  val list: List[Seq[Any]] = List(
    Seq(1, 2, 3),
    Seq(4, Float.NaN, 6),
    Seq(7, 8, 9)
  )

  // 方法一：使用 exists 结合模式匹配
  val result2 = list.filter(el => el.exists {
    case num: Float if num.isNaN => true
    case _ => false
  })

  // 方法二：保持原 contains 方法，确保类型正确
  // val result2 = list.filter(el => el.contains(Float.NaN))

  println(result2) // 输出: List(List(4, NaN, 6))
}
