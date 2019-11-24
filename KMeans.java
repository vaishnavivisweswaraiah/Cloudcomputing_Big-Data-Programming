import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {


  public static class KMMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private double [][] _centroids;
    private IntWritable cid = new IntWritable();

    public void setup(Mapper.Context context){
      Configuration conf = context.getConfiguration();
      //System.out.print(conf);
      String filename = conf.get("Centroids-file");
      _centroids = loadCentroids(filename, conf);
      //System.out.println(_centroids);
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	//Text data = new Text();
    	//String[] values=value.toString().split("\t");
    	//System.out.println(values[1]);
      double [] vec = parseVector(value.toString());
      cid.set(closest(vec));
      //System.out.println(value);
      //data.set(values[1]);
      context.write(cid, value);
    }


    private int closest(double [] v){
      double mindist = dist(v, _centroids[0]);
      int label =0;
      for (int i=1; i<_centroids.length; i++){
        double t = dist(v, _centroids[i]);
        if (mindist>t){
          mindist = t;
          label = i;
        }
      }
      return label;
    }


  }

  public static class KMReducer
       extends Reducer<IntWritable, Text, IntWritable, Text> {
    // write output: cid \t centroid_vector
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> vectors,
                       Context context
                       ) throws IOException, InterruptedException {
      double [] sum = null;
      int n=0;
      for (Text vec : vectors) {
        double [] v = parseVector(vec.toString());
        if (sum == null) sum = v;
        else 
          for (int i = 0; i < v.length; i++)
            sum[i] += v[i];
        n ++;
      }
      String out = Double.toString(sum[0]/n);
      for (int i = 1; i < sum.length; i ++ ){
        out +=  "," + Double.toString(sum[i]/n); // csv output
      } 
      result.set(out);
      context.write(key, result);
    }
  }

  // compute square Euclidean distance between two vectors v1 and v2
  public static double dist(double [] v1, double [] v2){
    double sum=0;
    for (int i=0; i< v1.length; i++){
      double d = v1[i]-v2[i];
      sum += d*d;
    }
    return Math.sqrt(sum);
  }

  // check convergence condition
  // max{dist(c1[i], c2[i]), i=1..numClusters < threshold
  private static boolean converge(double [][] c1, double [][] c2, double threshold){
    // c1 and c2 are two sets of centroids 
    double maxv = 0;
    for (int i=0; i< c1.length; i++){
        double d= dist(c1[i], c2[i]);
        if (maxv<d)
            maxv = d;
    } 

    if (maxv <threshold)
      return true;
    else
      return false;
    
  }


  public static double [][] loadCentroids(String filename, Configuration conf){

    double [][] centroids = null;
    Path p = new Path(filename);  // Path is used for opening the file. 
    try{
      //FileSystem fs = p.getFileSystem(conf);
      FileSystem fs = FileSystem.get(conf);//determines local or HDFS
      FSDataInputStream file = fs.open(p);
      byte[] bs = new byte[file.available()];
      file.read(bs);
      file.close();
      String [] lines = (new String(bs)).split("\n"); //lines are separated by \n
      for (String line:lines)
        System.out.println(line);
      centroids = new double[lines.length][];
      
      for (int i = 0; i < lines.length; i++){
        // cid \t centroid
        String [] parts = lines[i].split("\t");
        int cid = Integer.parseInt(parts[0]);
        centroids[cid] = parseVector(parts[1]);
      }
    }catch(Exception e){
        //log.error(e);
        System.out.println(e);
    }
    return centroids;
  }  

  public static double [] parseVector(String s){
    String [] itr = s.split(","); // comma separated
    double [] v = new double[itr.length];
    for (int i = 0; i < itr.length; i++)
    {
      v[i] = Double.parseDouble(itr[i]);
      //System.out.println(v[i]);
    }
    return v;
  }
  
  
  public static void main(String[] args) throws Exception {
    
    // usage: hadoop jar km.jar hdfs://localhost:9000/user/your_home_directory/centroids data.hdfs output
    Configuration conf = new Configuration();
    
    
    //args[0]="centroids"
    //args[1]="data.hdfs"
    //args[2]="Kmeans/kmeans_oup"
    
    int niteration=15;
    double threshold=0.001;
    double [][] _oldcentroids;
    
    FileSystem hdfs=FileSystem.get(conf);
    for (int i =0 ;i<niteration;i++)
    {
    	System.out.println("startiteration"+String.valueOf(i));
    	conf.set("Centroids-file", args[0]);
        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMMapper.class);
        job.setReducerClass(KMReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2])); 
        _oldcentroids=loadCentroids(conf.get("Centroids-file"), conf);
        //System.out.println("old");
    	//System.out.println(_oldcentroids);
    	
    	job.waitForCompletion(true);
    	hdfs.delete(new Path(args[0]),true);
    	FileUtil.copy(hdfs,new Path(args[2]+File.separator+"part-r-00000"),hdfs,new Path(args[0]),true,true,conf);
    	hdfs.delete(new Path(args[2]),true);
    	double[][] _newcentroids=loadCentroids(conf.get("Centroids-file"), conf);
    	//System.out.println("newcentoids");
    	//System.out.println(_newcentroids);
    	Boolean convergecondition=converge(_oldcentroids,_newcentroids,threshold);
    	if (convergecondition)
    	{
    		System.out.println("Centroids are similar ,converge condition is statisfied");
    		break;
    	}
   
    	
    	
    	
    }
    
  }
}