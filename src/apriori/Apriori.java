package apriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdata.HadoopCfg;

/*
* A parallel hadoop-based Apriori algorithm
*/
public class Apriori extends Configured implements Tool {


	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		String hdfsInputDir = "/input";
		String hdfsOutputDirPrefix = "/output/";
		int maxPasses = 2;
		int MIN_SUPPORT = 2;
		Integer MAX_NUM_TXNS = 4;
		System.out.println("InputDir : " + hdfsInputDir); //输入目录
		System.out.println("OutputDir Prefix : " + hdfsOutputDirPrefix);//输入目录
		System.out.println("Number of Passes : " + maxPasses); //K- 项集
		System.out.println("MinSup : " + MIN_SUPPORT); //最小支持度
		System.out.println("Max Txns : " + MAX_NUM_TXNS); //事务数目
		for (int passNum = 1; passNum <= maxPasses; passNum++) {
			boolean isPassKMRJobDone = runPassKMRJob(hdfsInputDir, hdfsOutputDirPrefix, passNum, MIN_SUPPORT,
					MAX_NUM_TXNS);
			if (!isPassKMRJobDone) {
				System.err.println("Phase1 MapReduce job failed. Exiting !!");
				return -1;
			}
		}
		return 1;
	}

	/*
	 * Runs the pass K mapreduce job for apriori algorithm.
	 */
	private static boolean runPassKMRJob(String hdfsInputDir, String hdfsOutputDirPrefix, int passNum,
			int MIN_SUPPORT, Integer MAX_NUM_TXNS)
					throws IOException, InterruptedException, ClassNotFoundException {
		boolean isMRJobSuccess = false;
		Configuration conf = HadoopCfg.getCfg();
		conf.setInt("passNum", passNum);
		conf.setInt("minSup", MIN_SUPPORT);
		conf.setInt("numTxns", MAX_NUM_TXNS);
		System.out.println("Starting AprioriPhase" + passNum + "Job");

		Job aprioriPassKMRJob = Job.getInstance(conf);
		if (passNum == 1) {
			configureAprioriJob(aprioriPassKMRJob, AprioriPass1Mapper.class);
		} else {
			configureAprioriJob(aprioriPassKMRJob, AprioriPassKMapper.class);
		}
		FileInputFormat.addInputPath(aprioriPassKMRJob, new Path(hdfsInputDir));
		FileOutputFormat.setOutputPath(aprioriPassKMRJob, new Path(hdfsOutputDirPrefix + passNum));
		isMRJobSuccess = (aprioriPassKMRJob.waitForCompletion(true) ? true : false);
		return isMRJobSuccess;
	}

	/*
	 * Phase1 - MapReduce
	 * 
	 * Configures a map-reduce job for Apriori based on the parameters like pass
	 * number, mapper etc.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void configureAprioriJob(Job aprioriJob, Class mapperClass) {
		aprioriJob.setJarByClass(Apriori.class);
		aprioriJob.setMapperClass(mapperClass);
		aprioriJob.setReducerClass(AprioriReducer.class);
		aprioriJob.setOutputKeyClass(Text.class);
		aprioriJob.setOutputValueClass(IntWritable.class);
	}

	/*
	 * Mapper for Phase1 would emit a <itemId, 1> pair for each item across all
	 * transactions.
	 */
	public static class AprioriPass1Mapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text item = new Text();
		public void map(Object key, Text txnRecord, Context context) throws IOException, InterruptedException {
			Transaction txn = AprioriUtils.getTransaction(txnRecord.toString());
			for (Integer itemId : txn.getItems()) {
				item.set(itemId.toString());
				context.write(item, one);
			}
		}
	}

	/*
	 * Reducer for all phases would collect the emitted itemId keys from all the
	 * mappers and aggregate it to return the count for each itemId.
	 */
	public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text itemset, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int countItemId = 0;
			for (IntWritable value : values) {
				countItemId += value.get();
			}
			String itemsetIds = itemset.toString();
			itemsetIds = itemsetIds.replace("[", "");
			itemsetIds = itemsetIds.replace("]", "");
			itemsetIds = itemsetIds.replace(" ", "");
			
			//如果候选频繁项集汇总后，出现次数比minsup 大，说明是频繁项 输出。
			Integer minSup = context.getConfiguration().getInt("minSup", 2);
			Integer numTxns = context.getConfiguration().getInt("numTxns", 12);
			if (AprioriUtils.hasMinSupport(minSup, numTxns, countItemId)) {
				context.write(new Text(itemsetIds), new IntWritable(countItemId));
			}
		}
	}

	// Phase2 - MapReduce
	/*
	 * Mapper for PhaseK would emit a <itemId, 1> pair for each item across all
	 * transactions.
	 */
	public static class AprioriPassKMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text item = new Text();

		private List<ItemSet> largeItemsetsPrevPass = new ArrayList<ItemSet>();
		private List<ItemSet> candidateItemsets = null;
		private HashTreeNode hashTreeRootNode = null;

		//初始化 从频繁（k-1）- 项集中生成候选 k- 项集
		@Override
		public void setup(Context context) throws IOException {
			
			//迭代次数参数
			int passNum = context.getConfiguration().getInt("passNum", 2);
			String opFileLastPass = "/output/"+(passNum - 1) + "/part-r-00000";
			try {
				Path pt = new Path(opFileLastPass);
				FileSystem fs = FileSystem.get(context.getConfiguration());
				BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String currLine = null;
				//对获取的频繁(k-1) -项集进行处理，以空格和制表符切分
				while ((currLine = fis.readLine()) != null) {
					currLine = currLine.trim();
					String[] words = currLine.split("[\\s\\t]+");
					if (words.length < 2) {
						continue;
					}
					List<Integer> items = new ArrayList<Integer>();
					for(int k=0; k < words.length -1 ; k++){
						String csvItemIds = words[k];
						String[] itemIds = csvItemIds.split(",");
						for(String itemId : itemIds) {
							items.add(Integer.parseInt(itemId));
						}
					}
					String finalWord = words[words.length - 1];
					int supportCount = Integer.parseInt(finalWord);
					largeItemsetsPrevPass.add(new ItemSet(items, supportCount));
				}
			} catch (Exception e) {
			}
			//生成候选 k- 项集
			candidateItemsets = AprioriUtils.getCandidateItemsets(largeItemsetsPrevPass, (passNum - 1));
			//对候选k- 项集构建HashTree ，加速查找
			hashTreeRootNode = HashTreeUtils.buildHashTree(candidateItemsets, passNum);
		}

		public void map(Object key, Text txnRecord, Context context) throws IOException, InterruptedException {
            //事务数据库读原始数据
			Transaction txn = AprioriUtils.getTransaction(txnRecord.toString());
			
			//事务数据库中查找候选项集，得到候选项集的出现次数统计
			List<ItemSet> candidateItemsetsInTxn = HashTreeUtils.findItemsets(hashTreeRootNode, txn, 0);
			for (ItemSet itemset : candidateItemsetsInTxn) {
				item.set(itemset.getItems().toString());
				context.write(item, one);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Apriori(), args);
		System.exit(exitCode);
	}

}

