import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //emits arrival airport code as key and arrival delay as value
        //emit(airportCode, arrDelay);
        String s = value.toString();
        String [] arr=s.split("\t");
        if(!(arr[15].isEmpty() ||arr[15]==null ||arr[15]==""))
            context.write(new Text(arr[9]), new Text(arr[15].trim()+","+1));//get  arrival airport code and arrival delay
        
       

    }

}