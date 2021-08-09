package homework4;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;


public class GeekInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        MyDemoRecordReader reader = new MyDemoRecordReader(
                new LineRecordReader(job, (FileSplit) genericSplit));
        return reader;
    }

    public static class MyDemoRecordReader implements
            RecordReader<LongWritable, Text> {

        LineRecordReader reader;
        Text text;

        public MyDemoRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }


        public void close() throws IOException {
            reader.close();
        }


        public LongWritable createKey() {
            return reader.createKey();
        }


        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return reader.getPos();
        }


        public float getProgress() throws IOException {
            return reader.getProgress();
        }


        public boolean next(LongWritable key, Text value) throws IOException {
            while (reader.next(key, text)) {

                String line = text.toString();

                Pattern p=Pattern.compile("ge{2,256}k");

                Matcher m=p.matcher(line);
                String strReplace = m.replaceAll("");
                Text txtReplace = new Text();
                txtReplace.set(strReplace);
                value.set(txtReplace.getBytes(), 0, txtReplace.getLength());
                return true;

            }
            return false;
        }
    }
}