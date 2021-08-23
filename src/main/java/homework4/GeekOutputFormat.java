//package homework4;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Random;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//import java.util.stream.Collectors;
//import java.io.IOException;
//import java.util.Properties;
//
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
//import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.TextOutputFormat;
//import org.apache.hadoop.util.Progressable;
//
//
//public class GeekOutputFormat<K extends WritableComparable, V extends Writable>
//        extends TextOutputFormat<K, V> implements HiveOutputFormat<K, V> {
//
//
//    public static int randInt(int min, int max) {
//
//
//        Random rand = new Random();
//
//        int randomNum = rand.nextInt((max - min) + 1) + min;
//
//        return randomNum;
//    }
//    public RecordWriter getHiveRecordWriter(JobConf job, Path outPath,
//                                            Class<? extends Writable> valueClass, boolean isCompressed,
//                                            Properties tableProperties, Progressable progress)
//            throws IOException {
//        FileSystem fs = outPath.getFileSystem(job);
//        FSDataOutputStream out = fs.create(outPath);
//
//        return new DocRecordWriter(out);
//    }
//    public static class DocRecordWriter implements RecordWriter {
//
//        private FSDataOutputStream out;
//
//
//        public DocRecordWriter(FSDataOutputStream o) {
//            this.out = o;
//        }
//
//        @Override
//        public void close(boolean abort) throws IOException {
//            out.flush();
//            out.close();
//        }
//
//        @Override
//        public void write(Writable wr) throws IOException {
//            String line = wr.toString();
//            List<String> wordList = new ArrayList<String>(Arrays.asList(line.split(" ")));
//            List<String> finalList = new ArrayList<String>(Arrays.asList(line.split(" ")));
//            int wordcount = 0;
//            int gekcount = 0;
//            int randno = randInt(2,256);
//            for (int i = 0; i < wordList.size(); i++) {
//                wordcount = wordcount +1;
//                if(i==randno){
//                    String e = StringUtils.repeat("e",wordcount);
//                    String gek = "g"+e+"k";
//                    finalList.add(i+1,gek);
//                    gekcount = gekcount+1;
//                    randno = randInt(2,256);
//                }
//
//            }
//            String outString = String.join(" ",finalList);
//            write(outString);
//
//        }
//
//        private void write(String str) throws IOException {
//            out.write(str.getBytes(), 0, str.length());
//        }
//
//    }
//}