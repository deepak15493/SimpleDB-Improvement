### Test Cases:

Just add following code any java file to test.
- For task 1
        
        
        
        
        BufferMgr bufferMgr= SimpleDB.init("simpleDB");
        new SimpleDB();
        bufferMgr=SimpleDB.bufferMgr();
        bufferMgr.clearBufferPool();
        bufferMgr.clearAppearances();

        //to print the contents of bufferpool at anytime
        //bufferMgr.printBufferPool();


        Block blk1=new Block("filename", 1);
        Block blk2=new Block("filename", 2);
        Block blk3=new Block("filename", 3);
        Block blk4=new Block("filename", 4);
        Block blk5=new Block("filename", 5);
        Block blk6=new Block("filename", 6);
        Block blk7=new Block("filename", 7);
        Block blk8=new Block("filename", 8);
        Block blk9=new Block("filename", 9);
        Block blk10=new Block("filename", 10);


        bufferMgr.pin(blk1);
        bufferMgr.pin(blk2);
        bufferMgr.pin(blk3);
        bufferMgr.pin(blk4);
        bufferMgr.pin(blk5);
        bufferMgr.pin(blk6);
        bufferMgr.unpin(blk7); 
        bufferMgr.pin(blk4);
        bufferMgr.pin(blk2);
        bufferMgr.pin(blk7);
        bufferMgr.pin(blk1);
        bufferMgr.unpin(blk8);
        bufferMgr.unpin(blk7);
        bufferMgr.unpin(blk6);
        bufferMgr.unpin(blk5);
        bufferMgr.unpin(blk4);
        bufferMgr.unpin(blk1);
        bufferMgr.unpin(blk7);
        bufferMgr.unpin(blk4);
        bufferMgr.unpin(blk2);
        bufferMgr.pin(blk9);
        bufferMgr.pin(blk10);	
        bufferMgr.printBufferPool();

- For task 2
  SimpleDB.logMgr().printLogPageBuffer();

