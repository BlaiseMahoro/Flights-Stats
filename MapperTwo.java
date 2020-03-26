import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTwo extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //emit(airportCode, (count, delay,avg,min, max, total,diffSquared)):
        String s = value.toString();
        String [] arr=s.split("\t");
        String key_str = arr[0];
        String []values= arr[1].split(",");
        int delay = Integer.parseInt(values[1].trim());
        int avg = Integer.parseInt(values[2].trim());
        double diff_squared =Math.pow((double)(delay-avg), 2.0);
    

        context.write(new Text(key_str), new Text(arr[1]+","+diff_squared)) ;//emit key: airport code and 
        //value:square of delay-avg
       

    }

}