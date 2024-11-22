## Ex.No:4 Big Data Analytics

## Aim:
To perform efficient data processing and distributed data analysis on large datasets using Apache Spark and Hadoop MapReduce, demonstrating their capabilities for handling big data.

## A. Data Processing with Spark
## Objective: 
Perform basic transformations and actions to handle large datasets efficiently using Apache Spark.

## Tools: Apache Spark, Python (PySpark)
## code:
```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Initialize Spark session
spark = SparkSession.builder.appName("DataProcessingExample").getOrCreate()

# Sample data (large dataset simulation)
data = [
    ("John", 28, "M", 3000),
    ("Jane", 35, "F", 3500),
    ("Sam", 50, "M", 4000),
    ("Anna", 23, "F", 2800),
    ("Peter", 40, "M", 5000),
    ("Alice", 29, "F", 3200),
    ("Bob", 31, "M", 3300)
]

# Create DataFrame from the data
columns = ["Name", "Age", "Gender", "Salary"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# 1. Filter - Get all males
males_df = df.filter(col("Gender") == "M")
males_df.show()

# 2. Group by Gender and calculate average salary
avg_salary_by_gender = df.groupBy("Gender").agg(avg("Salary").alias("AvgSalary"))
avg_salary_by_gender.show()

# 3. Count the number of people by Gender
count_by_gender = df.groupBy("Gender").agg(count("Name").alias("Count"))
count_by_gender.show()

# 4. Select specific columns and sort by salary
sorted_df = df.select("Name", "Salary").orderBy("Salary", ascending=False)
sorted_df.show()

# 5. Collect all records and print them (action)
records = df.collect()
print(records)

# Stop the Spark session
spark.stop()
```
## Explanation:
```
Transformations: Filtering, grouping, selecting, and sorting data.
Actions: Displaying and collecting processed data.
Spark Session: Manages distributed computations efficiently.
```
## B. Distributed Data Analysis with Hadoop
## Objective: 
Process data using the MapReduce paradigm and understand distributed storage mechanisms.

## Tools: Hadoop, Java
## B. Distributed Data Analysis with Hadoop
## Objective:
Process data using the MapReduce paradigm and understand distributed storage mechanisms.

## Tools: Hadoop, Java

Hadoop MapReduce Example (Word Count)
## Step 1: Mapper Class
Processes input data, splits it into words, and emits key-value pairs (word, 1).
## code:
```
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+");
        for (String wordText : words) {
            word.set(wordText);
            context.write(word, one);
        }
    }
}
```
## Step 2: Reducer Class
Aggregates counts for each word emitted by the Mapper.
## code:
```
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
```
## Step 3: Driver Class
Configures and runs the MapReduce job.
## code:
```
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
## Execution
![image](https://github.com/user-attachments/assets/748cdb89-70d8-423f-9060-a0e4f8458302)

## Result:
The Hadoop MapReduce Word Count job was executed successfully. It processed data in a distributed environment using HDFS, demonstrating effective data distribution and parallel processing
