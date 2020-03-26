import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, Text, Text, Text> {

   
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int count =0;
        int total =0;
        int min=Integer.MAX_VALUE;
        int max=Integer.MIN_VALUE;
        ArrayList<Integer> delays = new ArrayList<Integer>();

        
       //emit(airportCode,(count, delay,avg,min, max, total));
       for (Text v : values) {
         
                int delay= Integer.parseInt(v.toString().split(",")[0].trim());
                total +=delay;
                count+=Integer.parseInt(v.toString().split(",")[1].trim());
                if( delay < min){
                    min = delay;
                }
                if(delay > max){
                    max=delay;
                }
                delays.add(delay);

       }
       int avg = total/count;
       for(int delay :delays){
            String output = count +","+delay+","+avg+","+min+","+max+","+total;
            context.write(key, new Text(output));
        }
       
    }
}
