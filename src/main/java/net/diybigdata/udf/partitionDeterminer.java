package net.diybigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import java.util.*;
import java.io.*;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

@Description(
	name = "partitionClassifier",
	value = "_FUN_(InputDataType)",
	extended = "Example: select _FUN_(InputDataType) From tablename;")


public class partitionDeterminer extends UDF{
	public Hashtable<String,HashSet<Long>> mapper = null;
	public Hashtable<Long,String> mapper2 = null;
	public String paths = null;
	//public static Logger logger = Logger.getLogger(partitionDeterminer.class);
	public long itemId;
	public String defaultPath = "src/main/java/net/diybigdata/udf/property/basicConfiguration.txt";
	public String propConf = null;
	public BufferedReader loadedData = null;
	public String evaluate(long Itemid, String pathD, String configure, int choice){
		this.itemId = Itemid;
		this.paths = pathD;
		if(choice == 0)
			return itemIdKey(configure);
		else
			return partitionKey(configure);

	}
	public partitionDeterminer(){
		try{
			PropertyConfigurator.configure(this.defaultPath);
		}catch(Exception e){
		
			System.out.println("this constructor and logger is not properly configured");
		}
	}
	public String itemIdKey(String conf){
		if(this.itemId == 0 || this.paths == null ){
			return "default_partition";
		}
		String vals;
		long keys;
		//logger.info("starting application");
		if(mapper2 == null){
			if(this.propConf == null && conf != null){
				this.propConf = conf;
				PropertyConfigurator.configure(this.propConf);
			}
			//logger.info("initializing dictionary table");
			mapper2 = new Hashtable<Long,String>();
			try{
				loadedData = new BufferedReader(new FileReader(this.paths));
				String str = null;
				while((str = loadedData.readLine()) != null){
					String[] strArr = str.split("\t");
					keys = Long.parseLong(strArr[1].trim());
					vals = strArr[2].trim();
					if(mapper2.containsKey(keys) == false){
						mapper2.put(keys,vals);
					}
				}
			//logger.info("end of table initialization");
				loadedData.close();
			}catch(FileNotFoundException e){
				System.out.println("Given file is not found");
			//	logger.info("Given file is not foundi and information generated in line# 64");
				return null;
			}catch(IOException e){
			//	logger.info("Possible IO/stringtoint/inttostring formatting error information generated in line# 68");
				System.out.println("possible IO/arrayindex/stringtoint/inttostring formatting Error");
				return null;
			}catch(ArrayIndexOutOfBoundsException e){
			//	logger.info("possible array out of index problem and information generated in line# 72");
				System.out.println("array index is out of bounds ");
				e.printStackTrace();
			}
			finally{
				/*
				try{
					loadedData.close();
				}catch(IOException e1){
				//	logger.info("File stream might already be closed or never opened before");
					System.out.println("File Stream already closed");
					return null;	
				}*/
			}

		}

		if(mapper2.containsKey(this.itemId) == true){
			return (String) mapper2.get(this.itemId);
		}
			
		return "default_partition";

	}
	public String partitionKey(String conf){

		if(this.itemId == 0 || this.paths == null ){
			return "default_partition";
		}
	
		long vals;
		String keys;
		if(mapper == null){
			if(this.propConf == null && conf != null){
				this.propConf = conf;
				PropertyConfigurator.configure(this.propConf);
			}
			mapper = new Hashtable<String,HashSet<Long>>();
			//BufferedReader loadedData = null;
			try{
				loadedData = new BufferedReader(new FileReader(this.paths));
				String str = null;
				while((str = loadedData.readLine()) != null ){
					String[] strArr = str.split("\t");
					vals = Long.parseLong(strArr[1].trim());
					keys = strArr[2].trim();
					if(mapper.containsKey(keys) == false){
						HashSet temp = new HashSet<Long>();
						temp.add(vals);
						mapper.put(keys,temp);
					}else{
						HashSet tempset = mapper.get(keys);
						tempset.add(vals);
						mapper.put(keys,tempset);
					}
				}
			 	loadedData.close();
		
			}catch(FileNotFoundException e){
				//logger.info("file is missing and information generated at line# 125");
				System.out.println("Given file is not found");
				return null;

			}catch(IOException e){
				//logger.info("possible IO exception might be stringtoint, inttostring, io formatting error information generated in line# 131");
				System.out.println("possible io/arrayoutofindex/stringtoint/inttostring formatting Error");
				return null;
			}
			finally{
				/*
				try{
					loadedData.close();
				}catch(IOException e){
					//logger.info("file stream already closed");
					System.out.println("File Steream is already closed");
					return null;
				
				}*/
			}
		}
		for (Map.Entry es : mapper.entrySet()){
			HashSet temphash = (HashSet) es.getValue();
			if(temphash.contains(this.itemId) == true)
				return (String) es.getKey();
		}
		//if(mapper.containsKey(itemid) == true)
		//	return mapper.get(itemid);
		
		return "default_partition";
	}
//	public String evaluate(Long itemid){
		
	
//	}
}
