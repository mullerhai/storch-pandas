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

import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.reflect.InvocationTargetException
import java.util.Arrays as JArrays
import java.util.List as JList // Use alias for Java List and Arrays

// Import necessary Scala collections
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.* // For converting Java collections to Scala and vice versa

import org.jline.reader.Candidate
import org.jline.reader.Completer
import org.jline.reader.EndOfFileException
import org.jline.reader.LineReader
import org.jline.reader.LineReaderBuilder
import org.jline.reader.ParsedLine
import org.mozilla.javascript.Context
import org.mozilla.javascript.Function
import org.mozilla.javascript.NativeJavaArray
import org.mozilla.javascript.NativeJavaClass
import org.mozilla.javascript.Scriptable
import org.mozilla.javascript.ScriptableObject
import org.mozilla.javascript.WrappedException

import torch.DataFrame
import torch.pandas.operate.adapter.DataFrameAdapter

object Shell:

  /** Starts a Read-Eval-Print Loop (REPL) using standard input.
    *
    * @param frames
    *   A list of DataFrames to make available in the REPL context.
    * @return
    *   The result of the last evaluated expression.
    * @throws IOException
    *   If an I/O error occurs.
    */
  def repl(frames: List[DataFrame[AnyRef]]): Object =
    // Convert Scala List to Java List as the overloaded method expects Java List
    repl(System.in, frames)

  /** Starts a Read-Eval-Print Loop (REPL) using the specified input stream.
    *
    * @param input
    *   The input stream for the REPL.
    * @param frames
    *   A list of DataFrames to make available in the REPL context.
    * @return
    *   The result of the last evaluated expression.
    * @throws IOException
    *   If an I/O error occurs.
    */
  def repl(input: InputStream, frames: List[DataFrame[AnyRef]]): Object =
    new Repl(input, frames).run()

  // Define the inner Repl class
  private class Repl(input: InputStream, frames: List[DataFrame[AnyRef]])
      extends ScriptableObject:
    // Use val for constants
    private final val serialVersionUID: Long = 1L
    private final val PROMPT: String = "> "
    private final val PROMPT_CONTINUE: String = "  "
    private final val LAST_VALUE_NAME: String = "_"
    private final val FILENAME: String = "<shell>"
    private final val NEWLINE: String = System.getProperty("line.separator")

    // Use var for mutable state, transient is a JVM concept, keep it for fidelity
    @transient
    private var quitVar: Boolean = false
    @transient
    private var statement: Int = 1

    // Constructor is the primary constructor in Scala class definition

    // Override getClassName from ScriptableObject
    override def getClassName(): String = "shell"

    /** Runs the REPL loop.
      *
      * @return
      *   The result of the last evaluated expression.
      * @throws IOException
      *   If an I/O error occurs.
      */
    def run(): AnyRef =
      var result: AnyRef = null // Use Scala's Null type for nullable Object
      val console = consoleFunc(input)
      val ctx = Context.enter()

      val interactive = System.console() != null // Check interactivity here

      if (interactive) {
        val pkg = classOf[DataFrame[AnyRef]].getPackage // Use classOf[T] to get Class
        val rhino = classOf[Context].getPackage
        // Use Scala string interpolation for printf
        System.out.printf(s"# ${pkg.getImplementationTitle} ${pkg
            .getImplementationVersion}\n# ${System
            .getProperty("java.vm.name")}, ${System
            .getProperty("java.vendor")}, ${System
            .getProperty("java.version")}\n# ${rhino
            .getImplementationTitle} ${rhino.getImplementationVersion}\n")
      }

      try {
        ctx.initStandardObjects(this)
        // add functions
        // Use Scala Array for the function names
        defineFunctionProperties(
          Array[String]("print", "quit", "source"),
          getClass(),
          ScriptableObject.DONTENUM,
        )
        // make data frame easily available
        try ScriptableObject.defineClass(this, classOf[DataFrameAdapter]) // Use classOf[T]
        catch {
          case ex @ (_: IllegalAccessException | _: InstantiationException |
              _: InvocationTargetException) => throw new RuntimeException(ex) // Catch multiple exceptions in one block
        }
        // make data frame classes available as well
        // Iterate over Java Array using asScala
        for (cls <- classOf[DataFrame[AnyRef]].getDeclaredClasses.toSeq)
          put(cls.getSimpleName, this, new NativeJavaClass(this, cls))
        // make argument frames available
        // Keep as Java Array as it's used with NativeJavaArray
        val array = new Array[DataFrameAdapter](frames.size)
        // Iterate over Java List using asScala
        for (i <- 0 until frames.size) {
          val df = frames(i) // Get element from Java List
          array(i) = new DataFrameAdapter(
            ctx.newObject(this, df.getClass.getSimpleName),
            df,
          )
        }
        put("frames", this, new NativeJavaArray(this, array))

        var expr: String = null // Use Scala's Null type for nullable String
        // Use while loop as in original Java
        while (!quitVar && { expr = read(console); expr } != null)
          try {
            result = evalFunc(expr)
            if (result != Context.getUndefinedValue) {
              // store last value for reference
              put(LAST_VALUE_NAME, this, result)
              if (interactive) System.out.println(Context.toString(result))
            }
          } catch {
            case ex: Exception => // Catch all exceptions
              if (interactive) ex match {
                case wrapped: WrappedException =>
                  wrapped.getCause.printStackTrace() // Pattern match for WrappedException
                case _ => ex.printStackTrace()
              }
              result = ex // Store the exception as the result
          }
      } finally Context.exit() // Ensure Context is exited
      // Convert Rhino result to Java Object
      Context.jsToJava(result, classOf[Object])

    /** Reads a complete statement from the console.
      *
      * @param console
      *   The console to read from.
      * @return
      *   The complete statement string, or null if EOF is reached.
      * @throws IOException
      *   If an I/O error occurs.
      */
    def read(console: Console): String =
      val ctx = Context.getCurrentContext()
      val buffer = new StringBuilder() // Use Scala StringBuilder
      var line: String | Null = null // Use Scala's Null type for nullable String
      line = console.readLine(PROMPT) // Read the first line
      if (line != null) {
        // apply continued lines to last value
        if (line.startsWith(".") && has(LAST_VALUE_NAME, this)) buffer
          .append(LAST_VALUE_NAME)

        // read lines until a complete statement is found or eof
        buffer.append(line)
        // Use while loop as in original Java
        while (
          !ctx.stringIsCompilableUnit(buffer.toString()) &&
          { line = console.readLine(PROMPT_CONTINUE); line } != null
        ) buffer.append(NEWLINE).append(line)

        buffer.toString() // Return the accumulated string
      } else "" // null // Return null if the first line was null (EOF)

    /** Evaluates a JavaScript source string.
      *
      * @param source
      *   The JavaScript source string.
      * @return
      *   The result of the evaluation.
      */
    def evalFunc(source: String): AnyRef =
      val ctx = Context.getCurrentContext()
      statement += 1
      ctx.evaluateString(this, source, FILENAME, statement, null) // Use current statement value
      // Increment statement counter

    /** Rhino-callable print function.
      */
    @SuppressWarnings(Array("unused")) // Use Scala Array for annotation parameters
    def print(
        ctx: Context,
        scriptable: Scriptable,
        args: Array[AnyRef],
        func: Function,
    ): Unit =
      // Iterate over Scala Array
      for (i <- args.indices) {
        if (i > 0) System.out.print(" ")
        System.out.print(Context.toString(args(i))) // Access array element using ()
      }
      System.out.println()

    /** Rhino-callable quit function.
      */
    @SuppressWarnings(Array("unused"))
    def quit(): Unit = quitVar = true // Set the quit flag

    /** Rhino-callable source function to execute scripts from files.
      */
    @SuppressWarnings(Array("unused"))
    def source(
        ctx: Context,
        scriptable: Scriptable,
        args: Array[AnyRef],
        func: Function,
    ): Unit =
      // Cast object to Repl using asInstanceOf
      val repl = scriptable.asInstanceOf[Repl]
      // Iterate over Scala Array
      for (i <- args.indices) {
        val file = Context.toString(args(i))
        val sourceReader = new SourceReader(file) // Create inner class instance
        var expr: String = null // Use Scala's Null type for nullable String
        // Use while loop as in original Java
        val console: repl.Console = sourceReader.asInstanceOf[repl.Console]
        while ({ expr = repl.read(console); expr } != null) {
          val result = repl.evalFunc(expr)
          if (result != Context.getUndefinedValue) {
            // store last value for reference
            repl.put(LAST_VALUE_NAME, repl, result)
            val interactive = System.console() != null // Check interactivity here
            if (interactive) System.out.println(Context.toString(result))
          }
        }
      }

    /** Factory method to create a Console implementation.
      *
      * @param input
      *   The input stream.
      * @return
      *   A Console instance.
      * @throws IOException
      *   If an I/O error occurs.
      */
    private def consoleFunc(input: InputStream): Console =
      val interactive = System.console() != null // Check interactivity here
      if (interactive)
        try new JLineConsole() // Try to create JLineConsole
        catch {
          case ignored: NoClassDefFoundError => // Catch NoClassDefFoundError
            // Fallback to basic Console
            new Console(new BufferedReader(new InputStreamReader(input)))
        }
      else
        // Fallback to basic Console
        new Console(new BufferedReader(new InputStreamReader(input)))

    // Define the inner Console class
    class Console(reader: BufferedReader | Null): // Use Scala's Null type for nullable BufferedReader
      // Primary constructor takes an optional BufferedReader

      // Secondary constructor for JLineConsole (reader is null)
      // private def this() = this(null) // Example of secondary constructor

      /** Reads a line from the console with a prompt.
        *
        * @param prompt
        *   The prompt string to display.
        * @return
        *   The line read, or null if EOF is reached.
        * @throws IOException
        *   If an I/O error occurs.
        */
      def readLine(prompt: String): String | Null = {
        val interactive = System.console() != null // Check interactivity here
        if (interactive) System.out.print(prompt)
        // Use Option to safely call readLine on the nullable reader
        Option(reader).map(_.readLine()).orNull
      }

    // Define the inner SourceReader class extending Console
    class SourceReader(file: String)
        extends Console(new BufferedReader(new FileReader(file))):
      // Primary constructor calls super constructor

      // Override readLine from Console
      override def readLine(prompt: String): String | Null = {
        val interactive = System.console() != null // Check interactivity here
        val line = super.readLine("") // Call super method with empty prompt
        if (interactive && line != null)
          // Use Scala string interpolation
          System.out.printf(s"$prompt$line$NEWLINE")
        line // Return the line
      }

    // Define the inner JLineConsole class extending Console and implementing Completer
    private class JLineConsole extends Console(null) with Completer: // Pass null to Console constructor, mix in Completer
      // Use val for the immutable console field after initialization
      private val console: LineReader = {
        val name = classOf[DataFrame[Object]].getPackage.getName // Use classOf[T] to get Class
        // Use LineReaderBuilder from JLine
        LineReaderBuilder.builder().appName(name).completer(this) // Pass this instance as the completer
          .build()
      }

      // Override readLine from Console
      override def readLine(prompt: String): String | Null =
        try console.readLine(prompt) // Use JLine's readLine
        catch {
          case eof: EndOfFileException => null // Return null on EOF
        }

      // Implement complete from Completer interface
      override def complete(
          reader: LineReader,
          line: ParsedLine,
          candidates: JList[Candidate],
      ): Unit = {
        val expr = line.word().substring(0, line.wordCursor())
        val dot = expr.lastIndexOf('.') + 1
        if (dot > 1) {
          val sym = expr.substring(0, dot - 1)
          val value = get(sym, Repl.this) // Access outer class's get method
          if (value.isInstanceOf[ScriptableObject]) { // Check type using isInstanceOf
            val so = value.asInstanceOf[ScriptableObject] // Cast using asInstanceOf
            val ids = so.getAllIds // Get IDs from ScriptableObject
            // Use asScala to iterate over Java Array
            for (id <- ids.toSeq) {
              val candidate = s"$sym.$id" // Use Scala string interpolation
              // Add candidate to the Java List
              candidates.add(new Candidate(
                candidate,
                candidate,
                null,
                null,
                null,
                null,
                false,
              ))
            }
          }
        }
      }
