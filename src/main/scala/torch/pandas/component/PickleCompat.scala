package torch.pandas.component

import scala.collection.mutable
import torch.pickle.Unpickler
import torch.pickle.objects.MulitNumpyNdArray
import torch.pandas.DataFrame

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object PickleCompat {

  def readPickleForCaseClassSeq[T:ClassTag](picklePath: String): Seq[T] = {
    val unpickler = new Unpickler()
    val df = unpickler.load(picklePath)
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
