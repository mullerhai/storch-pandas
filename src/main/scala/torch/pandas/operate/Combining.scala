package torch.pandas.operate

/*
 * torch -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import torch.pandas.DataFrame
import torch.pandas.DataFrame.{JoinType, KeyFunction, compare}

import scala.collection.mutable
import scala.collection.mutable.{LinkedHashMap, ListBuffer}
import scala.jdk.CollectionConverters.*
import scala.util.control.Breaks.{break, breakable}

// Import assumed existing type
// Import Java reflection for Array.newInstance if needed (though not in this specific class)
// import java.lang.reflect.Array

object Combining:

  /** Performs a join operation between two DataFrames.
    *
    * @param left
    *   The left DataFrame.
    * @param right
    *   The right DataFrame.
    * @param how
    *   The type of join (INNER, LEFT, RIGHT, OUTER).
    * @param on
    *   An optional KeyFunction to generate join keys from rows. If null, uses
    *   the index.
    * @tparam V
    *   The type of the values in the DataFrames.
    * @return
    *   A new DataFrame representing the joined result.
    * @throws IllegalArgumentException
    *   if generated keys are not unique.
    */
  def join[V](
      left: DataFrame[V],
      right: DataFrame[V],
      how: JoinType,
      on: KeyFunction[V] | Null,
  ): DataFrame[V] = {
    val leftIt = left.getIndex.iterator
    val rightIt = right.getIndex.iterator
    // Use mutable.LinkedHashMap to preserve insertion order
    val leftMap = mutable.LinkedHashMap[Any, List[V]]()
    val rightMap = mutable.LinkedHashMap[Any, List[V]]()

    // Populate leftMap
    for (row <- left) { // Assuming DataFrame is Iterable[List[V]] or similar
      val name = leftIt.next()
      // Use Option for 'on' to handle null safely
      val key = Option(on) match {
        case Some(keyFunc) => keyFunc.apply(row)
        case None => name
      }
      // Check for duplicate keys and put the row
      if (leftMap.put(key, row.toList).isDefined) // put returns Option[List[V]]
        throw new IllegalArgumentException(s"generated key is not unique: $key")
    }

    // Populate rightMap
    for (row <- right) { // Assuming DataFrame is Iterable[List[V]] or similar
      val name = rightIt.next()
      val key = Option(on) match {
        case Some(keyFunc) => keyFunc.apply(row)
        case None => name
      }
      if (rightMap.put(key, row.toList).isDefined) // put returns Option[List[V]]
        throw new IllegalArgumentException(s"generated key is not unique: $key")
    }

    // Determine initial columns based on join type
    // Use mutable.ListBuffer for efficient additions and modifications
    val initialColumns = how match {
      case JoinType.RIGHT => right.getColumns.to(mutable.ListBuffer) // Convert Java List to Scala mutable ListBuffer
      case _ => left.getColumns.to(mutable.ListBuffer) // Convert Java List to Scala mutable ListBuffer
    }

    // Determine columns to append based on join type
    val columnsToAppend = how match {
      case JoinType.RIGHT => left.getColumns.to(mutable.ListBuffer) // Convert Java List to Scala mutable ListBuffer
      case _ => right.getColumns.to(mutable.ListBuffer) // Convert Java List to Scala mutable ListBuffer
    }

    // Process and append columns, handling duplicates by renaming
    for (column <- columnsToAppend) {
      val index = initialColumns.indexOf(column)
      if (index >= 0)
        // Handle duplicate column names
        initialColumns(index) match { // Use pattern matching on the element at the duplicate index
          case l: ListBuffer[?] => // If the existing column name is a Java List
            // Cast to Java List[Any] for modification
            val l1 = l.asInstanceOf[ListBuffer[Any]]
            l1.append(if (how != JoinType.RIGHT) "left" else "right")

            // Assuming 'column' is also a Java List in this case
            column.asInstanceOf[ListBuffer[Any]]
              .append(if (how != JoinType.RIGHT) "right" else "left")

          case existingColumnName => // If the existing column name is not a List
            // Rename the existing column in initialColumns
            initialColumns(index) = s"$existingColumnName _${
                if (how != JoinType.RIGHT) "left" else "right"
              }"

            // Rename the current column being processed (this modification to 'column' itself
            // doesn't affect the iteration, but the modified value is added later)
            // Note: In Scala, 'column' is a val, so we can't reassign it.
            // We'll create a new value for the potentially modified column name to add.
            val modifiedColumnName =
              s"$column _${if (how != JoinType.RIGHT) "right" else "left"}"
            initialColumns.append(modifiedColumnName) // Add the potentially modified column name
        }
      else
        // Column is not a duplicate, just append it
        initialColumns.append(column)
    }

    // Create the result DataFrame with the determined columns
    val df = new DataFrame[V](initialColumns.map(_.toString).toSeq*) // Convert Scala ListBuffer back to Java List

    // Populate the DataFrame based on the join type and maps
    val (primaryMap, secondaryMap) = how match {
      case JoinType.RIGHT => (rightMap, leftMap)
      case _ => (leftMap, rightMap)
    }

    for ((key, primaryRow) <- primaryMap) {
      val tmp = mutable.ListBuffer[V]() // Use mutable ListBuffer for the combined row
      tmp.addAll(primaryRow) // Add the row from the primary map

      val secondaryRowOption = secondaryMap.get(key) // Get the corresponding row from the secondary map

      // Check if secondary row exists or if it's not an INNER join (where missing secondary rows are excluded)
      if (secondaryRowOption.isDefined || how != JoinType.INNER) {
        val secondaryRow = secondaryRowOption.getOrElse {
          // If secondary row is missing, create a list of nulls with the size of the secondary DataFrame's columns
          val secondaryColsSize = how match {
            case JoinType.RIGHT => left.getColumns.size // If RIGHT join, secondary is left
            case _ => right.getColumns.size // If LEFT/INNER/OUTER, secondary is right
          }
          List.fill[V](secondaryColsSize)(null.asInstanceOf[V]) // Create a Scala List of nulls
        }
        tmp.addAll(secondaryRow) // Add the secondary row (or list of nulls)

        // Append the combined row to the result DataFrame
        df.append(key, tmp.toList) // Convert Scala ListBuffer to Scala List, then to Java List
      }
    }

    // Handle OUTER join: add rows from the secondary map that were not in the primary map
    if (how == JoinType.OUTER) {
      val (outerSecondaryMap, outerPrimaryMap) = how match {
        case JoinType.RIGHT => (leftMap, rightMap) // If RIGHT join, secondary for outer is left
        case _ => (rightMap, leftMap) // If LEFT/INNER/OUTER, secondary for outer is right
      }

      for ((key, secondaryRow) <- outerSecondaryMap)
        // Check if this key was NOT in the primary map (already processed above)
        if (!outerPrimaryMap.contains(key)) {
          val tmp = mutable.ListBuffer[V]() // Use mutable ListBuffer for the combined row

          // Add nulls for the primary part
          val primaryColsSize = how match {
            case JoinType.RIGHT => right.getColumns.size // If RIGHT join, primary for outer is right
            case _ => left.getColumns.size // If LEFT/INNER/OUTER, primary for outer is left
          }
          tmp.addAll(List.fill[V](primaryColsSize)(null.asInstanceOf[V])) // Add Scala List of nulls

          // Add the row from the secondary map
          tmp.addAll(secondaryRow)

          // Append the combined row to the result DataFrame
          df.append(key, tmp.toList) // Convert Scala ListBuffer to Scala List, then to Java List
        }
    }

    df // Return the resulting DataFrame
  }

  /** Performs a join operation based on specified column indices.
    *
    * @param left
    *   The left DataFrame.
    * @param right
    *   The right DataFrame.
    * @param how
    *   The type of join (INNER, LEFT, RIGHT, OUTER).
    * @param cols
    *   The column indices to join on.
    * @tparam V
    *   The type of the values in the DataFrames.
    * @return
    *   A new DataFrame representing the joined result.
    */
  def joinOn[V](
      left: DataFrame[V],
      right: DataFrame[V],
      how: JoinType,
      cols: Int*,
  ): DataFrame[V] = {
    // Create a KeyFunction using a Scala function literal
    val onFunction: KeyFunction[V] = new KeyFunction[V] {
      override def apply(value: Seq[V]): AnyRef = { // KeyFunction expects Java List
        val key = mutable.ListBuffer[V]() // Use mutable ListBuffer for the key
        for (col <- cols) key.append(value(col)) // Get element from Java List
        key.toList
        // Return an unmodifiable Java List as the key, matching original behavior
//        java.util.Collections.unmodifiableList(key.toList.asJava).asScala // Convert Scala ListBuffer to Scala List, then to Java List
      }

    }
    // Call the main join method
    join(left, right, how, onFunction)
  }

  /** Merges two DataFrames based on their non-numeric columns.
    *
    * @param left
    *   The left DataFrame.
    * @param right
    *   The right DataFrame.
    * @param how
    *   The type of join (INNER, LEFT, RIGHT, OUTER).
    * @tparam V
    *   The type of the values in the DataFrames.
    * @return
    *   A new DataFrame representing the merged result.
    */
  def merge[V](
      left: DataFrame[V],
      right: DataFrame[V],
      how: JoinType,
  ): DataFrame[V] = {
    // Get non-numeric columns as Scala Sets
    val leftNonNumericCols = left.nonnumeric.getColumns.toSet
    val rightNonNumericCols = right.nonnumeric.getColumns.toSet

    // Find the intersection of non-numeric columns
    val intersection = leftNonNumericCols.intersect(rightNonNumericCols)

    // Convert the Scala Set to an Array[Any] (corresponds to Object[] in Java)
    val columnsArray: Array[Any] = intersection.toArray

    // Reindex both DataFrames to keep only the intersection columns and perform the join
    join(
      left.reindex(columnsArray.asInstanceOf[Array[Object]]),
      right.reindex(columnsArray.asInstanceOf[Array[Object]]),
      how,
      null,
    )
  }

  /** Updates a destination DataFrame with values from other DataFrames.
    *
    * @param dest
    *   The destination DataFrame to update.
    * @param overwrite
    *   If true, overwrite existing non-null values in dest.
    * @param others
    *   The other DataFrames to take values from.
    * @tparam V
    *   The type of the values in the DataFrames.
    */
  // @SafeVarargs // Annotation not needed in Scala
  def update[V](
      dest: DataFrame[V],
      overwrite: Boolean,
      others: DataFrame[? <: V]*,
  ): Unit =
    // Iterate through columns and rows of the destination DataFrame
    breakable(
      for (col <- 0 until dest.size) // Assuming size() is column count
        for (row <- 0 until dest.length) // Assuming length() is row count
          // Check if overwrite is allowed or if the destination cell is null
          if (overwrite || dest.getFromIndex(row, col) == null)
            // Iterate through the other DataFrames
            for (other <- others)
              // Check if the other DataFrame has the corresponding cell
              if (col < other.size && row < other.length) {
                val value = other.getFromIndex(row, col)
                // If the value from the other DataFrame is not null, set it in dest and break
                if (value != null) {
                  dest.set(row, col, value)
                  break
                  // break // Scala equivalent of break (using scala.util.control.Breaks) or restructure
                  // For simplicity here, we'll just rely on the loop structure.
                  // A more idiomatic Scala way might involve finding the first non-null value.
                  // Let's use a simple flag or return for early exit from the inner loop if needed.
                  // Given the original Java 'break', we'll simulate it by finding the first value.
                  // Let's refactor this inner part slightly for better Scala style.
                }
              },
    )

  // Refactored inner loop to find the first non-null value:
//    for (col <- 0 until dest.size) {
//      for (row <- 0 until dest.length ) {
//        if (overwrite || dest.get(row, col) == null) {
//          // Find the first non-null value from 'others' at (row, col)
//          val firstNonNullValue: Option[V] = others.collectFirst {
//            case other if col < other.size && row < other.length => other.get(row, col)
//          }.filter(_ != null) // Filter out nulls from the collected values
//
//          // If a non-null value was found, set it in the destination
//          firstNonNullValue.foreach { value =>
//            dest.set(row, col, value)
//          }
//        }
//      }
//    }
//  }

  /** Concatenates multiple DataFrames vertically.
    *
    * @param first
    *   The first DataFrame.
    * @param others
    *   The other DataFrames to concatenate.
    * @tparam V
    *   The type of the values in the DataFrames.
    * @return
    *   A new DataFrame containing the concatenated data.
    */
  // @SafeVarargs // Annotation not needed in Scala
  def concat[V](first: DataFrame[V], others: DataFrame[? <: V]*): DataFrame[V] = {
    // Combine the first DataFrame and the others into a single Scala List
    val dfs: List[DataFrame[? <: V]] = first :: others.toList

    // Calculate total rows and collect all unique column names
    var totalRows = 0
    // Use mutable.LinkedHashSet to preserve column order from the first DataFrame encountered
    val columns = mutable.LinkedHashSet[Any]()
    for (df <- dfs) {
      totalRows += df.length // Assuming length() is row count
      for (c <- df.getColumns) // Iterate through Java List of columns
        columns.add(c)
    }

    // Convert the unique column names Set to a Scala List
    val newcols: List[Any] = columns.toList

    // Create the combined DataFrame with the determined columns and total rows
    // Assuming DataFrame constructor takes Java List of columns and reshape takes rows, cols
    val combined = new DataFrame[V](newcols.map(_.toString) *)
      .reshape(totalRows, newcols.size)

    // Populate the combined DataFrame
    var offset = 0 // Keep track of the row offset for each DataFrame
    for (df <- dfs) {
      // Convert the current DataFrame's columns (Java List) to a Scala List
      val dfCols: List[Any] = df.getColumns.toList

      // Iterate through the columns of the current DataFrame
      for (cIndex <- 0 until dfCols.size) {
        val colName = dfCols(cIndex)
        // Find the index of this column name in the new combined columns list
        val newcIndex = newcols.indexOf(colName)

        // If the column exists in the combined DataFrame (it should if collected correctly)
        if (newcIndex >= 0)
          // Copy data from the current DataFrame to the combined DataFrame
          for (r <- 0 until df.length) { // Iterate through rows of the current DataFrame
            val value = df.getFromIndex(r, cIndex) // Get value from current DataFrame
            combined.set(offset + r, newcIndex, value) // Set value in combined DataFrame at the correct offset and column
          }
      }
      // Update the row offset for the next DataFrame
      offset += df.length
    }

    combined // Return the combined DataFrame
  }
