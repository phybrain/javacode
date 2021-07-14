package homework1;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCount {
    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {
        /**
         * Map处理逻辑
         * Map输入类型为<key, value> -> <LongWritable, Text>
         * Map输出类型为<单词, 出现次数> -> <Text, IntWritable>
         **/

        // 每个单词出现次数都为1
        private final static IntWritable one = new IntWritable(1);
        // 解析出来每个单词用Text类型存储
        private Text word = new Text();


        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            /**
             * @Description: 重写map函数
             * @param: [key, value, output, reporter]
             * @return: void
             **/
            // Text类型转换成String类型
            String line = value.toString();
            // 分词器，将一行文本切分成多个单词
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                // 取出单词并进行输出
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * Reduce处理逻辑
         * Reduce输入类型为<Text, IntWritable>
         * Reduce输出类型为<Text, IntWritable>
         **/

        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            /**
             * @Description: 重写reduce函数
             * @param: [key, values, output, reporter]
             * @return: void
             **/
            // 统计词频并输出结果
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // 配置信息
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        // 文件路径信息
        FileInputFormat.setInputPaths(conf, new Path("hdfs://47.101.206.249:19000/user/student/phy"));
        FileOutputFormat.setOutputPath(conf, new Path("D:\\java_project\\course\\src\\main\\output\\"));
        // 执行
        JobClient.runJob(conf);
    }
}
