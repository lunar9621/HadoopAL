package bigdata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ����HDFS�ļ�ϵͳ
 * @author Phenix
 *
 */
public class HadoopUtil {
	
	/**
	 *  ����Ŀ¼
	 * @param dirPath  Ŀ¼��ַ
	 * @throws Exception
	 */
	public static void mkdir(String dirPath) throws Exception{
		Configuration config = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(config);
		fs.mkdirs(new Path(dirPath));
		fs.close();
	}
	
	/**
	 *  �����ļ�
	 * @param filePath
	 * @throws Exception
	 */
	public static void createFile(String filePath) throws Exception{
		Configuration config = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(config);
		fs.create(new Path(filePath));
		fs.close();
	}
	/**
	 *  ɾ���ļ���Ŀ¼
	 *  
	 * @param filePath �ļ���Ŀ¼·��
	 * @throws Exception
	 */
	public static void deleteFile(String filePath) throws Exception{
		Configuration config = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(config);
		//fs.delete(new Path(filePath), false);
		fs.deleteOnExit(new Path(filePath));
		fs.close();
	}
	/**
	 *  �����ļ�
	 *  
	 * @param path
	 * @throws Exception
	 */
	public static void listFile(String path) throws Exception{
		Configuration config = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(config);
		FileStatus[] status = fs.listStatus(new Path(path));
		for(FileStatus s : status){
			System.out.println(s.getPath().toString());
		}
		fs.close();
	}
	
	public static void upload(String src , String dest) throws Exception{
		Configuration config = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(config);
		fs.copyFromLocalFile(new Path(src),new Path(dest));
		fs.close();
	}
	
	public static void download(String src , String dest) throws Exception{
		Configuration config = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(config);
		fs.copyToLocalFile(new Path(src), new Path(dest));
		fs.close();
	}
	
	public static void setup(String filePath) throws Exception{
		Configuration config = HadoopCfg.getCfg();
		FileSystem fs = FileSystem.get(config);
		//fs.delete(new Path(filePath), false);
		fs.deleteOnExit(new Path(filePath));
		fs.close();
	}
	public static void main(String[] args) throws Exception{
		mkdir("/a/b");
		createFile("/a/b/Hello.java");
		//deleteFile("/a/b/Hello.java");
		//listFile("/");
		//upload("H:\\hello.txt","/hello.txt");
		//download("/hello.txt","H:\\hello2.txt");
	}
}