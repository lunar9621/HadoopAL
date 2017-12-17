package bigdata;
/**
 * º”‘ÿHadoop≈‰÷√Œƒº˛
 */
import org.apache.hadoop.conf.Configuration;

public class HadoopCfg {
private static Configuration cfg = null;
	
	public static Configuration getCfg(){
		if(cfg == null){
			cfg = new Configuration();
			cfg.addResource(HadoopCfg.class.getResource("core-site.xml"));
			cfg.addResource(HadoopCfg.class.getResource("hdfs-site.xml"));
			cfg.addResource(HadoopCfg.class.getResource("yarn-site.xml"));
		}
		return cfg;
	}
}
