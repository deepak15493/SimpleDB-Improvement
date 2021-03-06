package simpledb.log;

import simpledb.server.SimpleDB;
import simpledb.buffer.Buffer;
import simpledb.file.*;
import static simpledb.file.Page.*;

import java.util.*;

/**
 * The low-level log manager.
 * This log manager is responsible for writing log records
 * into a log file.
 * A log record can be any sequence of integer and string values.
 * The log manager does not understand the meaning of these
 * values, which are written and read by the
 * {@link simpledb.tx.recovery.RecoveryMgr recovery manager}.
 * @author Edward Sciore
 */

/**
* made changes to:
* LogMgr constructor
* appendNewBlock()
* appendVal()
* flush()
* currentLSN()
* finalizeRecord()
* getLastRecordPosition()
* setLastRecordPosition()
* 
* @author Team number L (according to fall 17 project team excel)
*/

public class LogMgr implements Iterable<BasicLogRecord> {
   /**
    * The location where the pointer to the last integer in the page is.
    * A value of 0 means that the pointer is the first value in the page.
    */
   public static final int LAST_POS = 0;

   private String logfile;
   public Block currentblk;
   private int currentpos;
   public Buffer logBuffer;

   /**
    * Creates the manager for the specified log file.
    * If the log file does not yet exist, it is created
    * with an empty first block.
    * This constructor depends on a {@link FileMgr} object
    * that it gets from the method
    * {@link simpledb.server.SimpleDB#fileMgr()}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until
    * {@link simpledb.server.SimpleDB#initFileMgr(String)}
    * is called first.
    * @param logfile the name of the log file
    */
   
   
   
   // Changed Log Manager constructor to initialize the logBuffer in the buffer pool
   public LogMgr(String logfile) {
      this.logfile = logfile;
      int logsize = SimpleDB.fileMgr().size(logfile);
      if (logsize == 0)
         appendNewBlock();
      else {
    	  currentblk = new Block(logfile, logsize-1);
          logBuffer = SimpleDB.bufferMgr().pin(currentblk);
          logBuffer.contents.read(currentblk);
          currentpos = getLastRecordPosition() + INT_SIZE;
          SimpleDB.bufferMgr().bufferMgr.bufferPoolMap.put(currentblk,logBuffer);
          logBuffer.pin();
      }
   }

   /**
    * Ensures that the log records corresponding to the
    * specified LSN has been written to disk.
    * All earlier log records will also be written to disk.
    * @param lsn the LSN of a log record
    */
   public void flush(int lsn) {
      if (lsn >= currentLSN())
         flush();
   }

   /**
    * Returns an iterator for the log records,
    * which will be returned in reverse order starting with the most recent.
    * @see java.lang.Iterable#iterator()
    */
   public synchronized Iterator<BasicLogRecord> iterator() {
      flush();
      return new LogIterator(currentblk);
   }

   
   // Printing the contents in the logBuffer block....which might raise an exception, hence, surrounded with try catch
   public void printLogPageBuffer()
   {
      try{
	      System.out.println("Buffer pinned to log : " + logBuffer.block().number());
	      System.out.println("Contents  " + logBuffer.toString()+" :");
	      Iterator<BasicLogRecord> it = iterator();
			while (it.hasNext()) {
				BasicLogRecord basicLogRecord = it.next();
				System.out.println(basicLogRecord.nextInt() + "   " + basicLogRecord.nextString());
			}  
      }
      catch(Exception e)
      {}
            
   }
   
   /**
    * Appends a log record to the file.
    * The record contains an arbitrary array of strings and integers.
    * The method also writes an integer to the end of each log record whose value
    * is the offset of the corresponding integer for the previous log record.
    * These integers allow log records to be read in reverse order.
    * @param rec the list of values
    * @return the LSN of the final value
    */
   public synchronized int append(Object[] rec) {
      int recsize = INT_SIZE;  // 4 bytes for the integer that points to the previous log record
      for (Object obj : rec)
         recsize += size(obj);
      if (currentpos + recsize >= BLOCK_SIZE){ // the log record doesn't fit,
         flush();        // so move to the next block.
         appendNewBlock();
      }
      for (Object obj : rec)
         appendVal(obj);
      finalizeRecord();
      return currentLSN();
   }

   /**
    * Adds the specified value to the page at the position denoted by
    * currentpos.  Then increments currentpos by the size of the value.
    * @param val the integer or string to be added to the page
    */
   
   // Appending the string in the block of the log Buffer
   private void appendVal(Object val) {
      if (val instanceof String)
         logBuffer.contents.setString(currentpos, (String)val);
      else
         logBuffer.contents.setInt(currentpos, (Integer)val);
      currentpos += size(val);
   }

   /**
    * Calculates the size of the specified integer or string.
    * @param val the value
    * @return the size of the value, in bytes
    */
   private int size(Object val) {
      if (val instanceof String) {
         String sval = (String) val;
         return STR_SIZE(sval.length());
      }
      else
         return INT_SIZE;
   }

   /**
    * Returns the LSN of the most recent log record.
    * As implemented, the LSN is the block number where the record is stored.
    * Thus every log record in a block has the same LSN.
    * @return the LSN of the most recent log record
    */
   
   //LSN of the block from log buffer will be returned
   private int currentLSN() {
      return SimpleDB.bufferMgr().bufferMgr.bufferPoolMap.get(currentblk).block().number();
   }

   /**
    * Writes the current page to the log file.
    */
   
   // This will write the contents of the log buffer 
   private void flush() {
      logBuffer.contents.write(currentblk);
   }

   /**
    * Clear the current page, and append it to the log file.
    */
   
   // Appending new logs to the file and then replacing the old block in the bufferpool with the updated block
   private void appendNewBlock() {
      setLastRecordPosition(0);
      currentpos = INT_SIZE;
      Block bl = logBuffer.contents.append(logfile);
      Buffer b=SimpleDB.bufferMgr().bufferMgr.bufferPoolMap.get(currentblk);
      b.assignToBlock(bl);
      SimpleDB.bufferMgr().bufferMgr.bufferPoolMap.remove(currentblk);
      SimpleDB.bufferMgr().bufferMgr.bufferPoolMap.put(bl,b);
      currentblk=bl;
   }

   /**
    * Sets up a circular chain of pointers to the records in the page.
    * There is an integer added to the end of each log record
    * whose value is the offset of the previous log record.
    * The first four bytes of the page contain an integer whose value
    * is the offset of the integer for the last log record in the page.
    */
   
  
   private void finalizeRecord() {
	   logBuffer.contents.setInt(currentpos, getLastRecordPosition());
	   setLastRecordPosition(currentpos);
	   currentpos += INT_SIZE;
   }

   private int getLastRecordPosition() {
      return logBuffer.contents.getInt(LAST_POS);
   }

   private void setLastRecordPosition(int pos) {
	   logBuffer.contents.setInt(LAST_POS, pos);
   }
}
