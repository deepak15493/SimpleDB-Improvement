package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */

/**
* made changes to following functions: 
* basicBufferMgr constructor
* flushAll()
* Pin()
* PineNew()
* Unpin()
* chooseUnpinnedBuffer()
* findExistingBuffer()
* 
* new functions added:
* printAppearance()
* clearAppearance()
* printBufferPool()
* clearBufferPool()
* 
* @author Team number L (according to fall 17 project team excel)
*/
public class BasicBufferMgr {
   //private Buffer[] bufferpool;
	/**
	* removed: private Buffer[] bufferpool
	* added: Map<Block, Buffer> bufferPoolMap
	* @author Team number L (according to fall 17 project team excel)
	*/
   private int numAvailable;
   public Map<Block, Buffer> bufferPoolMap;
   
   /**
    * added: Map<Block,List<Integer>> appearance
	* @author Team number L (according to fall 17 project team excel)
	*/
   //Holds pinTime for a block
   private Map<Block,List<Integer>> appearance=new HashMap<Block, List<Integer>>();   
   
   private int pinTime = 1;
   
   //Default k is 2 for LRU-2
   private int k=2;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   
   //Driver methods for Buffer manager
  //Method which print an appearance array
   
   public void printAppearance()
   {
	   System.out.println("Printing an appearance");
	   for(Block b:appearance.keySet())
	   {
		   System.out.println(b.number() + " : " + appearance.get(b).toString());
	   }
   }
   
   //Print a bufferPool
   
   public void printBufferPool()
   {   //System.out.println("Num available : "+numAvailable);
	   System.out.println("--------------Printing Buffer Pool--------------------");
	   for(Block b:bufferPoolMap.keySet())
	   {
		   System.out.println(b.number());
	   }
	   
	   System.out.println("--------------End  Pool--------------------");
   }
   
   //we need to clear garbage value
   //This method removes garbage values in buffer
   
    public void clearBufferPool()
    {
    	LogMgr logMgr=SimpleDB.logMgr();
    	Block blk=logMgr.currentblk;
    	Buffer buffer=logMgr.logBuffer;
    	bufferPoolMap = new HashMap<Block, Buffer>(8);
    	bufferPoolMap.put(blk,buffer);
        int initializationBlockNumber = -1;
        numAvailable = 7;
        for(int i=0; i<7; i++){
      	  Block block = new Block("empty",initializationBlockNumber);
      	  bufferPoolMap.put(block, new Buffer());
      	  initializationBlockNumber--;
        }
    }
    
    //clear an appearance array which is used to store pin time
   public void clearAppearances() {    	
    	appearance= new HashMap<Block, List<Integer>>();
    	pinTime=1;
    }


   
  //----------------End--------------------- 
   
   
   BasicBufferMgr(int numbuffs) {
      /**
       * bufferpool = new Buffer[numbuffs];
       * numAvailable = numbuffs;
       * for (int i=0; i<numbuffs; i++)
       * 	bufferpool[i] = new Buffer(); 
       */
	  
	   //Initialising a bufferPoolMap to hold block
      bufferPoolMap = new HashMap<Block, Buffer>(numbuffs);
      int initializationBlockNumber = -1;
      numAvailable = numbuffs;
      for(int i=0; i<numbuffs; i++){
    	  Block block = new Block("empty",initializationBlockNumber);
    	  bufferPoolMap.put(block, new Buffer());
    	  initializationBlockNumber--;
      }
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      /**
       * for (Buffer buff : bufferpool)
       * 	if (buff.isModifiedBy(txnum))
       * 		buff.flush();
       */
	   
	   for (Buffer buff: bufferPoolMap.values())
		   if(buff.isModifiedBy(txnum))
			   buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         if(buff.contents!=null && buff.contents.filemgr==null)
         {
        	 buff.contents.filemgr=SimpleDB.fileMgr();
         }
         buff.assignToBlock(blk);
         
         //to insert a block that is being pinned here into bufferPoolMap
         Iterator<Map.Entry<Block,Buffer>> iterator = bufferPoolMap.entrySet().iterator();
         while(iterator.hasNext()){
       	  Map.Entry<Block, Buffer> entry = iterator.next();
       	  if(entry.getValue().equals(buff))
       		  iterator.remove();
         }
         bufferPoolMap.put(blk, buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      
    // to maintain pin times of blocks
      List<Integer> list;
      
      /*
       In the following code , we check if the block is already in appearance list
       //if it is not , we add a new entry in appearance list with pinTime
        else we add time count in a list
      */
      //check if keys of appearance has block blk
      if(checkExistanceOfBlockInAppearance(blk))
      {  
    	  
    	  list=appearance.get(blk);
    	  list.add(pinTime++);
    	  //System.out.println("true blok number :"+blk.number()+"   block Name :"+blk.fileName()+"  "+list.toString());
      }
      else
      {   
    	  list= new ArrayList<Integer>();
    	  list.add(pinTime++);
    	  appearance.put(blk, list);
    	  //System.out.println("fals blok number :"+blk.number()+"   block Name :"+blk.fileName()+"  "+list.toString());
      }
      
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      Block newBlock = buff.assignToNew(filename, fmtr);
      Iterator<Map.Entry<Block,Buffer>> iterator = bufferPoolMap.entrySet().iterator();
      while(iterator.hasNext()){
    	  Map.Entry<Block, Buffer> entry = iterator.next();
    	  if(entry.getValue().equals(buff))
    		  iterator.remove();
      }
      bufferPoolMap.put(newBlock, buff);
      numAvailable--;
      buff.pin();
      
      //adding to appearance
      List<Integer> list;
      
      //check if keys of appearance has block blk
      if(checkExistanceOfBlockInAppearance(newBlock))
      {  
    	  //System.out.println("true blok number :"+blk.number()+"   block Name :"+blk.fileName());
    	  list=appearance.get(newBlock);
    	  list.add(pinTime++);
    	  //System.out.println("true blok number :"+newBlock.number()+"   block Name :"+newBlock.fileName()+"  "+list.toString());
      }
      else
      {   
    	  list= new ArrayList<Integer>();
    	  list.add(pinTime++);
    	  appearance.put(newBlock, list);
    	  //System.out.println("fals blok number :"+newBlock.number()+"   block Name :"+newBlock.fileName()+"  "+list.toString());
      }

      
      return buff;
   }
   
//Driver method for above block
   //check if particular block is in the appearance list
   public boolean checkExistanceOfBlockInAppearance(Block block)
   {
	   boolean flag=false;
	   
	   for(Block b:appearance.keySet())
	   {
		   if(b.fileName().equals(block.fileName())   &&    b.number()==block.number())
		   {
			   flag=true;
			   return flag;
		   }
	   }
	   
	   
	   return flag;
   }
   
   
   private Buffer findExistingBuffer(Block blk) {
	   /**
	    * for (Buffer buff : bufferpool) {
	    * 	Block b = buff.block();
	    *	if (b != null && b.equals(blk)) 
	    * 		return buff;
	    *	} 
	    *	return null; 
	    */
	   
	   if(bufferPoolMap.containsKey(blk))
		   return bufferPoolMap.get(blk);
		return null;
   }
//ends
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   synchronized Buffer unpin(Block blk) {
	   if(bufferPoolMap.containsKey(blk))
	   {
		   Buffer buff = bufferPoolMap.get(blk);
		   return buff;
	   }
	   return new Buffer();
	   /*
	   for (Buffer buff : bufferPoolMap.values())
	         if (buff.block().equals(blk))
	         {
	        	buff.unpin();
	        	numAvailable++;
	        	break;
	         }
	   */      
   }

   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
 
   
   /**
    * private Buffer chooseUnpinnedBuffer() {
    * 	for (Buffer buff : bufferpool)
    * 		if (!buff.isPinned())
    * 			return buff;
    * 		return null;
    * 	} 
    */
   

   
   private Buffer chooseUnpinnedBuffer() {
	   
	   
	   for(Block blk: bufferPoolMap.keySet()){
		   Buffer buff = bufferPoolMap.get(blk);
		   if(!buff.isPinned() && blk.fileName().equalsIgnoreCase("Empty"))
			   return buff;
	   }
	   
	   if(checkSize())
	   {
		   k=2;
	   }
	   else {k=1;}
		   //use LRU-k
		   //find the second last time for every element and take min
	       //System.out.println("Value of k : "+k);
		   Block minBlock=null;
		   Buffer finalBuffer=null;
		   int minCounter=Integer.MAX_VALUE; 
		   for(Block block: appearance.keySet())
			  {   
			   List<Integer> list=appearance.get(block);
				      
					  if(list.size()!=0 && list.get(list.size()-k)<minCounter)
					  {
						  Buffer buffer=bufferPoolMap.get(block);
						  if(buffer!=null && !buffer.isPinned())
						  {
							     minCounter=list.get(list.size()-k);
							     minBlock=block;
							     finalBuffer=buffer;
						  }
					  }
				  
			  }
		   return finalBuffer;

	   
      /*for (Buffer buff : bufferpool)
         if (!buff.isPinned())
         return buff;*/
   }

   /**
    * Determines whether list size for each buffer
    */
   //helper method for chooseUnpinnedBuffer
   /*
   check if the every list in appearance is of size 2
   if size=2 retyrn true
   else false
   
   */
   boolean checkSize()
   { 
	   boolean flag=true;
       //System.out.println("Printing appearance from size");
       //printAppearance();
	   for(Block x: appearance.keySet())
	   {
		   
		   if(appearance.get(x)==null || appearance.get(x).size()<=1)
		   {flag=false;}
		}
	  
	   //System.out.println("Flag  : "+flag);
	   return flag;
   }
   
   /**
    * Determines whether the map has a mapping from
    * the block to some buffer.
    * @param blk the block to use as a key
    * @return true if there is a mapping, false otherwise
    */
   boolean containsMapping(Block blk){
	   return bufferPoolMap.containsKey(blk);
   }
   
   /**
    * Returns the buffer that the map maps the specified block to.
    * @param blk the block to use as a key
    * @return the buffer mapped to if there is a mapping, null otherwise
    */
   Buffer getMapping(Block blk){
	   return bufferPoolMap.get(blk);
   }
}