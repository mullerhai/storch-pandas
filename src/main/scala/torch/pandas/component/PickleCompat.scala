package torch.pandas.component

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import torch.pandas.DataFrame
import torch.pickle.Unpickler
import torch.pickle.objects.MulitNumpyNdArray
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
object PickleCompat {

  private val logger = LoggerFactory.getLogger(this.getClass)
  
  def readPickleForCaseClassSeq[T: ClassTag](picklePath: String): Seq[T] = {
    val unpickler = new Unpickler()
    val df: MulitNumpyNdArray = unpickler.load(picklePath).asInstanceOf[MulitNumpyNdArray]
    df.asInstanceOf[ListBuffer[T]].toSeq
  }

  def readPickleForMap(picklePath: String): mutable.HashMap[String, AnyRef] = {
    val unpickler = new Unpickler()
    val df = unpickler.load(picklePath)
    df.asInstanceOf[mutable.HashMap[String, AnyRef]]
  }

  def readPickleForNumpy(picklePath: String): MulitNumpyNdArray = {
    val unpickler = new Unpickler()
    val df = unpickler.load(picklePath)
    df.asInstanceOf[MulitNumpyNdArray]
  }

}
