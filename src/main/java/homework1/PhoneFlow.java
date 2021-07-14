package homework1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class PhoneFlow {

    public static class FlowMap extends MapReduceBase implements
            Mapper<LongWritable, Text, Text,FlowBean> {

        private Text text = new Text();
        private FlowBean flowBean = new FlowBean();
        public void map(LongWritable longWritable, Text value, OutputCollector<Text,FlowBean> outputCollector, Reporter reporter) throws IOException {

            String[] words = value.toString().split("\t");

            String phone = words[1];
            text.set(phone);
            Long upFlow = Long.parseLong(words[words.length-3]);
            Long downFlow = Long.parseLong(words[words.length-2]);
            Long sum = upFlow + downFlow;
            flowBean.setUpFlow(upFlow);
            flowBean.setDownFlow(downFlow);
            flowBean.setSumFlow(sum);
            outputCollector.collect(text,flowBean);
        }

    }

    public static class FlowReduce extends MapReduceBase implements Reducer<Text,FlowBean,Text,FlowBean>{

        private FlowBean out = new FlowBean();
        public void reduce(Text text, Iterator<FlowBean> values, OutputCollector<Text, FlowBean> outputCollector, Reporter reporter) throws IOException {
            long sumUpFlow=0;
            long sumDownFlow=0;

            while (values.hasNext()) {

                FlowBean flowBean = values.next();
                sumUpFlow+=flowBean.getUpFlow();
                sumDownFlow+=flowBean.getDownFlow();

            }

            out.setUpFlow(sumUpFlow);
            out.setDownFlow(sumDownFlow);
            out.setSumFlow(sumDownFlow+sumUpFlow);
            outputCollector.collect(text,out);


        }
    }

    public static void main(String[] args) throws Exception {
        // 配置信息
        String inpath = args[0];
        String outpath = args[1];
//        String outpath = "D:\\java_project\\course\\src\\main\\output\\";
//        "D:\\java_project\\course\\src\\main\\phoneflow"
        System.out.println("input:"+inpath);
        System.out.println("output:"+outpath);
        FileUtil.deleteDir(outpath);
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("phoneflow");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FlowBean.class);
        conf.setMapperClass(FlowMap.class);
        conf.setCombinerClass(FlowReduce.class);
        conf.setReducerClass(FlowReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        // 文件路径信息
        FileInputFormat.setInputPaths(conf, new Path(inpath));
        FileOutputFormat.setOutputPath(conf, new Path(outpath));
        // 执行
        JobClient.runJob(conf);
    }
}
