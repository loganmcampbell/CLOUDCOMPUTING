import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.LongWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//RAWCOMPARATOR FOR COMPARING TEXT PAIR BYTE REPRESENTATIONS
//HADOOP PAGE 100 and 101 : THE DEFINITIVE GUIDE **
public class NewText extends WritableComparator
{

    public static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    public NewText()
    {
      // USE TEXT CLASS NOT TEXT.PAIR
        super(Text.class);
    }


    //One may optimize compare-intensive operations by overriding compare(byte[],int,int,byte[],int,int).
    //Static utility methods are provided to assist in optimized implementations of this method.

    // THIS IS FROM PAGE 101 on the Hadoop: THE DEFINITIVE GUIDE!
    // Dealing with Custom Comparators
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
    {
        try
        {
          int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1);
          int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2);
          //SINCE THE ORIGINAL SORTING IS ALPHA. READING THAT TIMES THE VALUES BY A NEGATIVE
          //REVERESES THE ORDER RATHER THAN LISTING EACH KEY PAIR AND THEN SORTIN IT
          //IT WOULD HAVE TAKEN LONGER TO RUN
          return (-1) * TEXT_COMPARATOR.compare(b1,s1,l1,b2,s2,l2);



        }
        catch(IOException e)
        {
          throw new IllegalArgumentException(e);
        }
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        if (w1 instanceof Text && w2 instanceof Text)
        {
            return (-1) *(((Text) w1).compareTo((Text) w2));
        }

        return super.compare(w1,w2);
    }
}
