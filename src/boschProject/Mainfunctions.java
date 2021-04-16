package boschProject;



import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.common.io.Files;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

import org.apache.hadoop.fs.FileSystem;

import scala.Tuple2;



//import org.apache.spark.SparkConf;

//import org.apache.spark.api.java.JavaRDD;

//import org.apache.spark.api.java.JavaSparkContext;

public class Mainfunctions {
    
	static int globalcount = 0;
	static int calledind = 0;
	static int calledindadd =0; 
	//This is the search criteria to be used in Spark
	static String searchCriteria="";
	
	
	public static void  processSparkRDD(String FileName) {
		//boolean result = false;
		System.setProperty("hadoop.home.dir", "C:/Hadoop");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		
		SparkConf sparkConfig = new SparkConf().setAppName("Gategories RDD").setMaster("local[*]");

        // start a spark context

        JavaSparkContext sparkCon = new JavaSparkContext(sparkConfig);
        JavaRDD<String> InitialRDD = sparkCon.textFile(FileName);

        
        /*
         * Doing transformations to be used to access the required lookups. 
         */

        // Create a paired list to be used later 

        JavaPairRDD<String, String> DocumentsList = InitialRDD.mapToPair(inputline -> {
        	String[] cols =inputline.split("=");
        	String Key = cols[0];
        	String Value = cols[1];
        	return new Tuple2<>(Key, Value);
        });

        //System.out.println("\n After transformation   "+ DocumentsList.getNumPartitions());
        
        //We need to group all keys so we can search the document using the keys
        
        //JavaPairRDD<String, Iterable<String>> DocumentsListgroup = DocumentsList.groupByKey();
        
        
        
        
        sparkCon.close();
		
	}
	
	public boolean searchRDD(JavaPairRDD<String, Iterable<String>> document, String function) {
		boolean result = false;
		
		
		//Search functions
		return result;
		
		
	}
	
	public boolean transformRDD(ArrayList< Entry<String, Object> > listToCreate, String transformfunction) {
		boolean result = false;
		
		return result;
		
		
	}
	
	
	private static boolean exactMatch(String source, String subItem){
        String pattern = "\\b"+subItem+"\\b";
        Pattern p=Pattern.compile(pattern);
        Matcher m=p.matcher(source);
        return m.find();
   }
	
	public static ArrayList< Entry<String, Object> > addItemsToList(ArrayList< Entry<String, Object> > listToUpdate , String Name, String Type){
		int i;
		int matchind = 0;
		boolean matchflag = false;
		boolean typeflag = false;
		int typeflagcon=0;
		String newName; 
		String entrykey;
		String newVal;
		calledind++;
		Entry<String, Object> NewkeyVal;
		Entry<String, Object> MatchkeyVal;
		String MatchKey="";
		String MatchValue="";
		String typevalue="";
		String categoriesKey = "elements.category.categories[0]";
		int listsize = listToUpdate.size();
		
		
		
		
	try {
		for (  i=0; i< listsize; i++ ) {
			
			   
			   NewkeyVal = listToUpdate.get(i);
			   entrykey = NewkeyVal.getKey();
			   newVal = NewkeyVal.getValue().toString();
			 
			   if ( entrykey.contentEquals(categoriesKey) & typevalue.contentEquals(Type)) {
				   matchflag = true;
				   MatchkeyVal= NewkeyVal;
				   MatchKey = entrykey;
				   MatchValue = newVal;
			   }
			   else if ( entrykey.contentEquals("type")) {
				   
				   typevalue = newVal;
				   if (newVal.contentEquals(Type)) {
					   typeflag = true;
					   typeflagcon++;
					   
				   
				   
			   }
			   }
			   else if (entrykey.contentEquals("name")) {
				   
					   
				   //Check if the categories element is already available then don't add
				   
				   if (typeflag & !matchflag) {
					  
						   Entry<String,Object> newEntry =
				        		    new AbstractMap.SimpleEntry<String, Object>(categoriesKey, Name);
				           listToUpdate.add(i-1, newEntry);
				           typeflag = false;
				           typevalue="";
				           matchflag=false;
				           i=i+2;
				           calledindadd++;
				           
				           typeflagcon=0;
					   
				   }
				  
				   
				   
			  }
		}
	
	} catch (Exception e) {
		e.printStackTrace();
	}	
		 
	return listToUpdate; 	
}
	
	public static ArrayList< Entry<String, Object> > flattenJson (String jsonstring, String jsonType) {
		
	
	JSONParser parser = new JSONParser();
	int i =0;
    int j =0;
    
    
    Entry<String, Object> keyVal;
    
    Object Value; 
    ArrayList< Entry<String, Object> > NewlistOfEntry
    = new ArrayList<Entry<String, Object> >();
	
	try {

		
		Object obj = parser.parse(jsonstring);
		
		JSONObject jsonObject = (JSONObject) obj;
		
	
		
		
		//System.out.println("\n=====Simple Flatten===== \n" + flattenedJson);

		Map<String, Object> flattenedJsonMap = JsonFlattener.flattenAsMap(jsonObject.toString());

		// Set of the entries from the
        // HashMap
        Set<Entry<String, Object> > entrySet
            = flattenedJsonMap.entrySet();
  
        // Creating an ArrayList of Entry objects
        ArrayList<Entry<String, Object> > listOfEntry
            = new ArrayList<Entry<String, Object> >(entrySet);
        
         
        
        int mapSize = flattenedJsonMap.size();
        
        
        for ( i=0; i<mapSize; i++) {
           
         
            keyVal= listOfEntry.get(i); 
            String entrykey=keyVal.getKey();
            Value=keyVal.getValue();
            
            if (i>0) {
            	if ( jsonType == "documents") {
            		
            	              if(exactMatch(keyVal.getKey(),"document")) 
            	              {
            		            entrykey=entrykey.substring(22);
            	                 if ( entrykey.startsWith("."))
            	    	             entrykey=entrykey.substring(1);
            	               }
            	              else if(exactMatch(keyVal.getKey(),"documents"))
            	              {
         		                 entrykey=entrykey.substring(13);
         	
         	                      if ( entrykey.startsWith(".")) 
         	    	                entrykey=entrykey.substring(1);
 	
                             }
            	             else 
            	               {
            		                 entrykey=entrykey.substring(13);
            	
            	                      if ( entrykey.startsWith(".")) 
            	    	                entrykey=entrykey.substring(1);
    	
                                }
            	            }
            	
            	else if(jsonType == "categories") {
            		entrykey=entrykey.substring(13);
            	        if ( entrykey.startsWith(".")) 
	                        entrykey=entrykey.substring(1);
            	}
             }
            Entry<String,Object> Newkey =
        		    new AbstractMap.SimpleEntry<String, Object>(entrykey, Value);
        	
        	
            NewlistOfEntry.add(i, Newkey);
        	
        	
     
        
        }   
       
        
        
	} catch (Exception e) {
		e.printStackTrace();
	}
	return NewlistOfEntry;
	
} 
	
	
	public static void main(String[] args)  {
		
		
		// FIRST: Read JSON files
		
		
	   
		//Read first JSON document: Documents
		//String documents will store the JSON data streamed from Documents JSON in string format
				
				String documents = ""; 
				try
				{
					String documentsURL = "https://content-us-1.content-cms.com/api/06b21b25-591a-4dd3-a189-197363ea3d1f/delivery/v1/search?"
							+ "q=classification:content&fl=document:[json]&fl=type&rows=100";
					
					URL urlDocuments = new URL(documentsURL);
					
					HttpsURLConnection conn1 = (HttpsURLConnection)urlDocuments.openConnection();
					
					conn1.setRequestMethod("GET");
				
					conn1.connect();
				
					int responsecode = conn1.getResponseCode();
					
					
					//Iterating condition to if response code is not 200 then throw a runtime exception
					//else continue the actual process of getting the JSON data
					if(responsecode != 200)
						throw new RuntimeException("HttpResponseCode: " +responsecode);
					else
					{
						
						
						Scanner sc1 = new Scanner(urlDocuments.openStream());
						while(sc1.hasNext())
						{
							documents+=sc1.nextLine();
						}
						
						sc1.close();
						
						
					}
					
				//Disconnect the HttpURLConnection stream
					conn1.disconnect();
				}
				catch(Exception e1)
				{
					e1.printStackTrace();
				}

				//Read Second JSON document: Categories
				//String categories will store the JSON data streamed from Documents JSON in string format
						
				String categories = "";
				try
				 {
							String categoriesURL="https://content-us-1.content-cms.com/api/06b21b25-591a-4dd3-a189-197363ea3d1f/delivery/v1/search?"
									+ "q=classification:category&rows=100";
							URL urlCategories = new URL(categoriesURL);
							
							HttpsURLConnection conn2 = (HttpsURLConnection)urlCategories.openConnection();
							
							conn2.setRequestMethod("GET");
							
							conn2.connect();
							
							int responsecode = conn2.getResponseCode();
							
							
							//Iterating condition to if response code is not 200 then throw a runtime exception
							//else continue the actual process of getting the JSON data
							if(responsecode != 200)
								throw new RuntimeException("HttpResponseCode: " +responsecode);
							else
							{
								
								Scanner sc2 = new Scanner(urlCategories.openStream());
								while(sc2.hasNext())
								{
									categories+=sc2.nextLine();
								}
								
								sc2.close();
							}
						
							conn2.disconnect();
					}
				catch(Exception e2)
					{
							e2.printStackTrace();
					}
	
	/*
	 * Merging both files into one file called Merged. 
	 * Before we merge them we first flatten the files so they can be parsed as key, value combinations into an array list
	 * that can be processed later on. 
	 * After the merger we put file in an output file to be processed later in SparkRDD			
	 */
	
	//Merged file is initialized to first JSON file: documents
	  
	  
	
		ArrayList< Entry<String, Object> > flattenedDocumentsJson
	    = new ArrayList<Entry<String, Object> >();
		
		ArrayList< Entry<String, Object> > flattenedCategoriesJson
	    = new ArrayList<Entry<String, Object> >();
		
		
		
		
		/**
		 * Flattend JSON files and produce an array for each
		 */
		
		// Flattend Documents Json
		flattenedDocumentsJson = flattenJson(documents, "documents");
		
		// Flattend Categories Json
		flattenedCategoriesJson = flattenJson(categories,"categories");
		
		
		/**
		 * Merge the two returned arrays and add missing categories for each document
		 */
		
		
		int index;
		int indexCategories;
		
		
			  
	    //if document.elements.category doesn't exist then we need to search in the categories JSON file and get
		// the matching category of the document from the Path key.
	    // Matching is done by comparing the 
		           
	    String eventPage="Dynamic list criteria/Item types/Event page";
	    String sampleArticle1 ="Sample Article/Tech";
	    String sampleArticle2 ="Sample Article/Lifestyle";
	    String sampleArticle3 ="Sample Article/Fashion";
	    String sampleArticle4 ="Sample Article/Travel";
	    String rightsManaged = "Usage rights/Rights managed/";
		String shutterStock = "Usage rights/Shutterstock/";
		String recipePage= "Dynamic list criteria/Item types/Recipe page";
		String []categoriesTypes= new String[51]; 
		String catval="";
		String key="";
		String keydoc="";
		String keydocVal="";
		int categoriescount=0;
		
		ArrayList< Entry<String, Object> > updatedArray
		   	        = new ArrayList<Entry<String, Object> >();
		updatedArray=flattenedDocumentsJson;
		           
		 for(indexCategories=0; indexCategories<flattenedCategoriesJson.size(); indexCategories++) {
			 key = flattenedCategoriesJson.get(indexCategories).getKey();
			 if(key.contentEquals("path")){
				 catval = flattenedCategoriesJson.get(indexCategories).getValue().toString();
				 
				 //for (int j=0; j<categoriesTypes.length; j++) {
					 if(categoriescount==0) {
							 categoriesTypes[categoriescount]=catval;
							 categoriescount++;
					 }
					 else if (categoriesTypes[categoriescount] == null) {
						 String s = categoriesTypes[categoriescount-1];
						 
						   if ( !(s.contains(catval)) ) {
							      categoriesTypes[categoriescount] = catval;
						          categoriescount++;
					         }
					     }
					 
				 
				
			 }
		 } 
		 
			 int ind;
			 int first = 0;
			 
				 for ( int k=0; k<categoriescount; k++) {
						
						
				  	 if(categoriesTypes[k].contains(eventPage)) {
			       		   
			       			
			       			updatedArray  = addItemsToList(flattenedDocumentsJson, eventPage, "Event page");
			       			//remove category type from the category list after being processed
			       			categoriesTypes[k]="";
			       			
			       			
				  	      }
			       	      			       	    	 
			       	   if(categoriesTypes[k].contains(sampleArticle1) ) {
			       	    		
				        	   updatedArray  = addItemsToList(flattenedDocumentsJson, sampleArticle1, "Sample Article");
				        	   categoriesTypes[k]="";
				        	    		 
				        	    		
			       	    	  }
			       	    	  else if(categoriesTypes[k].contains(sampleArticle2)) {
			       	    		  
			       	    		
					        	  updatedArray  = addItemsToList(flattenedDocumentsJson, sampleArticle2, "Sample Article");
					        	  categoriesTypes[k]="";
					        	    	
					        	       
			       	    	  }
			       	    	  else if(categoriesTypes[k].contains(sampleArticle3)) {
			       	    		
					        	    	 
					        	   updatedArray  = addItemsToList(flattenedDocumentsJson, sampleArticle3, "Sample Article");
					        	   categoriesTypes[k]="";
					        	    	
					        	        
			       	    	  }
			       	    	  
			       	    	  else if(categoriesTypes[k].contains(sampleArticle4)) {
			       	    		
					        	    updatedArray  = addItemsToList(flattenedDocumentsJson, sampleArticle4, "Sample Article");
					        	    categoriesTypes[k]="";
					        	    	 
					        	      
			       	    	  }
			       	     
			       }//end for categories
			 
				 //}//end if type
	       	     
		    
	       	   
		 
		// System.out.println("============  Merged Files ============");
		 //System.out.println(updatedArray);	 
		 
		 
		  	   
		  	   
		
		
		 
         //}End for loop
				 // Write array to a file so we can process it in Spark
				 try {
					 FileWriter writer = new FileWriter("src/resources/mergedfile.txt");
				        int size = updatedArray.size();
				        for (int i=0;i<size;i++) {
				            String str = updatedArray.get(i).toString();
				            writer.write(str);
				            //if(i < size-1)**//This prevent creating a blank like at the end of the file**
				                writer.write("\n");
				        }
				        writer.close();
				    }
					    
					catch(Exception ex) {
					    ex.printStackTrace();
					}
				 
				 // Create the RDD file and do transformations in it
				 processSparkRDD("src/resources/mergedfile.txt");
			
	}

}	

  

