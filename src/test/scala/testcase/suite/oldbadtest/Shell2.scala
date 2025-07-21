package testcase.suite.oldbadtest

///*
// * storch -- Data frames for Java
// * Copyright (c) 2014, 2015 IBM Corp.
// *
// * This program is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package torch.pandas.operate
//
//import java.io.BufferedReader
//import java.io.FileReader
//import java.io.IOException
//import java.io.InputStream
//import java.io.InputStreamReader
//import java.lang.reflect.InvocationTargetException
//import java.util
//import org.jline.reader.Completer
//import org.jline.reader.Candidate
//import org.jline.reader.EndOfFileException
//import org.jline.reader.LineReader
//import org.jline.reader.LineReaderBuilder
//import org.jline.reader.ParsedLine
//import torch.DataFrame
//import org.mozilla.javascript.Context
//import org.mozilla.javascript.Function
//import org.mozilla.javascript.NativeJavaArray
//import org.mozilla.javascript.NativeJavaClass
//import org.mozilla.javascript.Scriptable
//import org.mozilla.javascript.ScriptableObject
//import org.mozilla.javascript.WrappedException
//import torch.pandas.operate.adapter.DataFrameAdapter
//import scala.collection.mutable.{LinkedHashMap,ListBuffer, *}
//object Shell2 {
//  @throws[IOException]
//  def repl(frames:  Seq[DataFrame[AnyRef]]): AnyRef = repl(System.in, frames)
//
//  @throws[IOException]
//  def repl(input: InputStream, frames:  Seq[DataFrame[AnyRef]]): AnyRef = new Shell2.Repl(input, frames).run
//
//  @SerialVersionUID(1L)
//  private object Repl {
//    private val PROMPT = "> "
//    private val PROMPT_CONTINUE = "  "
//    private val LAST_VALUE_NAME = "_"
//    private val FILENAME = "<shell>"
//
//    @SuppressWarnings(Array("unused")) def print(ctx: Context, scriptable: Scriptable, args: Array[AnyRef], func: Function): Unit = {
//      for (i <- 0 until args.length) {
//        if (i > 0) System.out.print(" ")
//        System.out.print(Context.toString(args(i)))
//      }
//      System.out.println()
//    }
//
//    @SuppressWarnings(Array("unused"))
//    @throws[Exception]
//    def source(ctx: Context, scriptable: Scriptable, args: Array[AnyRef], func: Function): Unit = {
//      val repl = classOf[Shell2.Repl].cast(scriptable)
//      for (i <- 0 until args.length) {
//        val file = Context.toString(args(i))
//        val source = new Shell2.Repl#SourceReader(file)
//        var expr: String = null
//        while ((expr = repl.read(source)) != null) {
//          val result = repl.eval(expr)
//          if (result ne Context.getUndefinedValue) {
//            // store last value for reference
//            repl.put(LAST_VALUE_NAME, repl, result)
//            if (repl.interactive) System.out.println(Context.toString(result))
//          }
//        }
//      }
//    }
//  }
//
//  @SerialVersionUID(1L)
//  private class Repl ( val input: InputStream,  val frames:  Seq[DataFrame[AnyRef]]) extends ScriptableObject {
//    final private val NEWLINE = System.getProperty("line.separator")
//    final private val interactive = System.console != null
//    @transient private var quit = false
//    @transient private var statement = 1
//
//    override def getClassName = "shell"
//
//    @throws[IOException]
//    def run: AnyRef = {
//      var result: AnyRef = null
//      val console2 = console(input)
//      val ctx = Context.enter
//      if (interactive) {
//        val pkg = classOf[DataFrame[?]].getPackage
//        val rhino = classOf[Context].getPackage
//        System.out.printf("# %s %s\n# %s, %s, %s\n# %s %s\n", pkg.getImplementationTitle, pkg.getImplementationVersion, System.getProperty("java.vm.name"), System.getProperty("java.vendor"), System.getProperty("java.version"), rhino.getImplementationTitle, rhino.getImplementationVersion)
//      }
//      try {
//        ctx.initStandardObjects(this)
//        // add functions
//        defineFunctionProperties(Array[String]("print", "quit", "source"), getClass, ScriptableObject.DONTENUM)
//        // make data frame easily available
//        try ScriptableObject.defineClass(this, classOf[DataFrameAdapter])
//        catch {
//          case ex@(_: IllegalAccessException | _: InstantiationException | _: InvocationTargetException) =>
//            throw new RuntimeException(ex)
//        }
//        // make data frame classes available as well
//        for (cls <- classOf[DataFrame[?]].getDeclaredClasses) {
//          put(cls.getSimpleName, this, new NativeJavaClass(this, cls))
//        }
//        // make argument frames available
//        val array = new Array[DataFrameAdapter](frames.size)
//        for (i <- 0 until frames.size) {
//          val df = frames(i)
//          array(i) = new DataFrameAdapter(ctx.newObject(this, df.getClass.getSimpleName), df)
//        }
//        put("frames", this, new NativeJavaArray(this, array))
//        var expr: String = null
//        while (!quit && (expr = read(console2)) != null) try {
//          result = eval(expr)
//          if (result ne Context.getUndefinedValue) {
//            // store last value for reference
//            put(Repl.LAST_VALUE_NAME, this, result)
//            if (interactive) System.out.println(Context.toString(result))
//          }
//        } catch {
//          case ex: Exception =>
//            if (interactive) if (ex.isInstanceOf[WrappedException]) classOf[WrappedException].cast(ex).getCause.printStackTrace()
//            else ex.printStackTrace()
//            result = ex
//        }
//      } finally Context.exit()
//      Context.jsToJava(result, classOf[AnyRef])
//    }
//
//    @throws[IOException]
//    def read(console: Shell2.Repl#Console): String = {
//      val ctx = Context.getCurrentContext
//      val buffer = new StringBuilder
//      var line: String = null
//      if ((line = console.readLine(Repl.PROMPT)) != null) {
//        // apply continued lines to last value
//        if (line.startsWith(".") && has(Repl.LAST_VALUE_NAME, this)) buffer.append(Repl.LAST_VALUE_NAME)
//        // read lines a complete statement is found or eof
//        buffer.append(line)
//        while (!ctx.stringIsCompilableUnit(buffer.toString) && (line = console.readLine(Repl.PROMPT_CONTINUE)) != null) buffer.append(NEWLINE).append(line)
//        return buffer.toString
//      }
//      null
//    }
//
//    def eval(source: String): AnyRef = {
//      val ctx = Context.getCurrentContext
//      ctx.evaluateString(this, source, Repl.FILENAME, {
//        statement += 1; statement - 1
//      }, null)
//    }
//
//    @SuppressWarnings(Array("unused"))
//    def quit(): Unit = {
//      quit = true
//    }
//
//    @throws[IOException]
//    private def console(input: InputStream): Shell2.Repl#Console = {
//      if (interactive) try return new Shell2.Repl#JLineConsole
//      catch {
//        case ignored: NoClassDefFoundError =>
//      }
//      new Shell2.Repl#Console(new BufferedReader(new InputStreamReader(input)))
//    }
//
//    private class Console @throws[IOException]
//
//    private
//    {
//      this.reader = null
//      final  var reader: BufferedReader = null
//
//      def this(reader: BufferedReader) {
//        this()
//        this.reader = reader
//      }
//
//      @throws[IOException]
//      def readLine(prompt: String) = {
//        if (interactive) System.out.print(prompt)
//        reader.readLine
//      }
//    }
//
//    private class SourceReader @throws[IOException]
//
//    private file: String
//    extends Shell2.Repl
//    #Console(new BufferedReader(new FileReader(file))) {
//      @throws[IOException]
//      override def readLine(prompt: String) = {
//        val line = super.readLine("")
//        if (interactive && line != null) System.out.printf("%s%s\n", prompt, line, NEWLINE)
//        line
//      }
//    }
//
//    private class JLineConsole @throws[IOException]
//
//    private extends Shell2.Repl
//    #Console
//    with Completer {
//      val name = classOf[DataFrame[?]].getPackage.getName
//      console = LineReaderBuilder.builder.appName(name).completer(this).build
//      final private var console: LineReader = null
//
//      @throws[IOException]
//      override def readLine(prompt: String) = try console.readLine(prompt)
//      catch {
//        case eof: EndOfFileException =>
//          null
//      }
//
//      override def complete(reader: LineReader, line: ParsedLine, candidates:  Seq[Candidate]): Unit = {
//        val expr = line.word.substring(0, line.wordCursor)
//        val dot = expr.lastIndexOf('.') + 1
//        if (dot > 1) {
//          val sym = expr.substring(0, dot - 1)
//          val value = get(sym, thisRepl)
//          if (value.isInstanceOf[ScriptableObject]) {
//            val so = value.asInstanceOf[ScriptableObject]
//            val ids = so.getAllIds
//            for (id <- ids) {
//              val candidate = sym + "." + id
//              candidates.add(new Candidate(candidate, candidate, null, null, null, null, false))
//            }
//          }
//        }
//      }
//    }
//  }
//
//
//}
