package edu.escuelaing.alda.idioma;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class LangCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        JSONParser parser = new JSONParser();
        try {
            JSONObject json = (JSONObject) parser.parse(line);
            String idioma = (String) json.get("lang");
            output.collect(new Text(idioma), new IntWritable(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
