import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//import sun.launcher.resources.launcher;

public class ReducerTwo extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //emit(airportCode, (delay, stdev, avg,min, max, total))
        double tot_squ_diff =0;
        ArrayList<Integer> delays = new ArrayList<Integer>();
        String [] arr={};
       for (Text v : values) {
           arr=v.toString().split(",");
         
          tot_squ_diff+=Double.parseDouble(arr[6].trim());
          int delay= Integer.parseInt(v.toString().split(",")[1].trim());
          delays.add(delay);
       }
      
       int count = Integer.parseInt(arr[0].trim());
       double stdev = Math.sqrt(tot_squ_diff/count);

       for(int delay :delays){
        String output = delay+","+stdev+","+count+","+arr[2]+","+arr[3]+","+arr[4]+","+arr[5];
        context.write(key, new Text(output));
    }
   

       
       
       
    }
}
