// This file was automatically generated with the by org.lemurproject.galago.tupleflow.typebuilder.TypeBuilderMojo ...
package org.lemurproject.galago.core.types;

import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.protocol.*;
import org.lemurproject.galago.tupleflow.error.*;
import org.lemurproject.galago.utility.*;
import java.io.*;
import java.util.*;
import gnu.trove.list.array.*;

/**
 * Tupleflow-Typebuilder automatically-generated class: IndexSplit.
 */
@SuppressWarnings({"unused","unchecked"})
public final class IndexSplit implements Type<IndexSplit> {
    public String index;
    public long begId;
    public long endId; 
    
    /** default constructor makes most fields null */
    public IndexSplit() {}
    /** additional constructor takes all fields explicitly */
    public IndexSplit(String index, long begId, long endId) {
        this.index = index;
        this.begId = begId;
        this.endId = endId;
    }  
    
    public String toString() {
            return String.format("%s,%d,%d",
                                   index, begId, endId);
    } 

    public Order<IndexSplit> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+index", "+begId" })) {
            return new IndexBegIdOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<IndexSplit> {
        public void process(IndexSplit object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class IndexBegIdOrder implements Order<IndexSplit> {
        public int hash(IndexSplit object) {
            int h = 0;
            h += CmpUtil.hash(object.index);
            h += CmpUtil.hash(object.begId);
            return h;
        } 
        public Comparator<IndexSplit> greaterThan() {
            return new Comparator<IndexSplit>() {
                public int compare(IndexSplit one, IndexSplit two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.index, two.index);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.begId, two.begId);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<IndexSplit> lessThan() {
            return new Comparator<IndexSplit>() {
                public int compare(IndexSplit one, IndexSplit two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.index, two.index);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.begId, two.begId);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<IndexSplit> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<IndexSplit> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<IndexSplit> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< IndexSplit > {
            IndexSplit last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(IndexSplit object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.index, last.index)) { processAll = true; shreddedWriter.processIndex(object.index); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.begId, last.begId)) { processAll = true; shreddedWriter.processBegId(object.begId); }
               shreddedWriter.processTuple(object.endId);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<IndexSplit> getInputClass() {
                return IndexSplit.class;
            }
        } 
        public ReaderSource<IndexSplit> orderedCombiner(Collection<TypeReader<IndexSplit>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<IndexSplit> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public IndexSplit clone(IndexSplit object) {
            IndexSplit result = new IndexSplit();
            if (object == null) return result;
            result.index = object.index; 
            result.begId = object.begId; 
            result.endId = object.endId; 
            return result;
        }                 
        public Class<IndexSplit> getOrderedClass() {
            return IndexSplit.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+index", "+begId"};
        }

        public static String[] getSpec() {
            return new String[] {"+index", "+begId"};
        }
        public static String getSpecString() {
            return "+index +begId";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processIndex(String index) throws IOException;
            public void processBegId(long begId) throws IOException;
            public void processTuple(long endId) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastIndex;
            long lastBegId;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processIndex(String index) {
                lastIndex = index;
                buffer.processIndex(index);
            }
            public void processBegId(long begId) {
                lastBegId = begId;
                buffer.processBegId(begId);
            }
            public final void processTuple(long endId) throws IOException {
                if (lastFlush) {
                    if(buffer.indexs.size() == 0) buffer.processIndex(lastIndex);
                    if(buffer.begIds.size() == 0) buffer.processBegId(lastBegId);
                    lastFlush = false;
                }
                buffer.processTuple(endId);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeLong(buffer.getEndId());
                    buffer.incrementTuple();
                }
            }  
            public final void flushIndex(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getIndexEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getIndex());
                    output.writeInt(count);
                    buffer.incrementIndex();
                      
                    flushBegId(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushBegId(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getBegIdEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getBegId());
                    output.writeInt(count);
                    buffer.incrementBegId();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushIndex(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> indexs = new ArrayList<String>();
            TLongArrayList begIds = new TLongArrayList();
            TIntArrayList indexTupleIdx = new TIntArrayList();
            TIntArrayList begIdTupleIdx = new TIntArrayList();
            int indexReadIdx = 0;
            int begIdReadIdx = 0;
                            
            long[] endIds;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                endIds = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processIndex(String index) {
                indexs.add(index);
                indexTupleIdx.add(writeTupleIndex);
            }                                      
            public void processBegId(long begId) {
                begIds.add(begId);
                begIdTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(long endId) {
                assert indexs.size() > 0;
                assert begIds.size() > 0;
                endIds[writeTupleIndex] = endId;
                writeTupleIndex++;
            }
            public void resetData() {
                indexs.clear();
                begIds.clear();
                indexTupleIdx.clear();
                begIdTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                indexReadIdx = 0;
                begIdReadIdx = 0;
            } 

            public void reset() {
                resetData();
                resetRead();
            } 
            public boolean isFull() {
                return writeTupleIndex >= batchSize;
            }

            public boolean isEmpty() {
                return writeTupleIndex == 0;
            }                          

            public boolean isAtEnd() {
                return readTupleIndex >= writeTupleIndex;
            }           
            public void incrementIndex() {
                indexReadIdx++;  
            }                                                                                              

            public void autoIncrementIndex() {
                while (readTupleIndex >= getIndexEndIndex() && readTupleIndex < writeTupleIndex)
                    indexReadIdx++;
            }                 
            public void incrementBegId() {
                begIdReadIdx++;  
            }                                                                                              

            public void autoIncrementBegId() {
                while (readTupleIndex >= getBegIdEndIndex() && readTupleIndex < writeTupleIndex)
                    begIdReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getIndexEndIndex() {
                if ((indexReadIdx+1) >= indexTupleIdx.size())
                    return writeTupleIndex;
                return indexTupleIdx.get(indexReadIdx+1);
            }

            public int getBegIdEndIndex() {
                if ((begIdReadIdx+1) >= begIdTupleIdx.size())
                    return writeTupleIndex;
                return begIdTupleIdx.get(begIdReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getIndex() {
                assert readTupleIndex < writeTupleIndex;
                assert indexReadIdx < indexs.size();
                
                return indexs.get(indexReadIdx);
            }
            public long getBegId() {
                assert readTupleIndex < writeTupleIndex;
                assert begIdReadIdx < begIds.size();
                
                return begIds.get(begIdReadIdx);
            }
            public long getEndId() {
                assert readTupleIndex < writeTupleIndex;
                return endIds[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getEndId());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexIndex(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processIndex(getIndex());
                    assert getIndexEndIndex() <= endIndex;
                    copyUntilIndexBegId(getIndexEndIndex(), output);
                    incrementIndex();
                }
            } 
            public void copyUntilIndexBegId(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processBegId(getBegId());
                    assert getBegIdEndIndex() <= endIndex;
                    copyTuples(getBegIdEndIndex(), output);
                    incrementBegId();
                }
            }  
            public void copyUntilIndex(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getIndex(), other.getIndex());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processIndex(getIndex());
                                      
                        if (c < 0) {
                            copyUntilIndexBegId(getIndexEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilBegId(other, output);
                            autoIncrementIndex();
                            break;
                        }
                    } else {
                        output.processIndex(getIndex());
                        copyUntilIndexBegId(getIndexEndIndex(), output);
                    }
                    incrementIndex();  
                    
               
                }
            }
            public void copyUntilBegId(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getBegId(), other.getBegId());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processBegId(getBegId());
                                      
                        copyTuples(getBegIdEndIndex(), output);
                    } else {
                        output.processBegId(getBegId());
                        copyTuples(getBegIdEndIndex(), output);
                    }
                    incrementBegId();  
                    
                    if (getIndexEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilIndex(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<IndexSplit>, ShreddedSource {
            public ShreddedProcessor processor;
            Collection<ShreddedReader> readers;       
            boolean closeOnExit = false;
            boolean uninitialized = true;
            PriorityQueue<ShreddedReader> queue = new PriorityQueue<ShreddedReader>();
            
            public ShreddedCombiner(Collection<ShreddedReader> readers, boolean closeOnExit) {
                this.readers = readers;                                                       
                this.closeOnExit = closeOnExit;
            }
                                  
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof IndexSplit.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((IndexSplit.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<IndexSplit>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<IndexSplit> getOutputClass() {
                return IndexSplit.class;
            }
            
            public void initialize() throws IOException {
                for (ShreddedReader reader : readers) {
                    reader.fill();                                        
                    
                    if (!reader.getBuffer().isAtEnd())
                        queue.add(reader);
                }   

                uninitialized = false;
            }

            public void run() throws IOException {
                initialize();
               
                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    ShreddedReader next = null;
                    ShreddedBuffer nextBuffer = null; 
                    
                    assert !top.getBuffer().isAtEnd();
                                                  
                    if (queue.size() > 0) {
                        next = queue.peek();
                        nextBuffer = next.getBuffer();
                        assert !nextBuffer.isAtEnd();
                    }
                    
                    top.getBuffer().copyUntil(nextBuffer, processor);
                    if (top.getBuffer().isAtEnd())
                        top.fill();                 
                        
                    if (!top.getBuffer().isAtEnd())
                        queue.add(top);
                }              
                
                if (closeOnExit)
                    processor.close();
            }

            public IndexSplit read() throws IOException {
                if (uninitialized)
                    initialize();

                IndexSplit result = null;

                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    result = top.read();

                    if (result != null) {
                        if (top.getBuffer().isAtEnd())
                            top.fill();

                        queue.offer(top);
                        break;
                    } 
                }

                return result;
            }
        } 
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<IndexSplit>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            IndexSplit last = new IndexSplit();         
            long updateIndexCount = -1;
            long updateBegIdCount = -1;
            long tupleCount = 0;
            long bufferStartCount = 0;  
            ArrayInput input;
            
            public ShreddedReader(ArrayInput input) {
                this.input = input; 
                this.buffer = new ShreddedBuffer();
            }                               
            
            public ShreddedReader(ArrayInput input, int bufferSize) { 
                this.input = input;
                this.buffer = new ShreddedBuffer(bufferSize);
            }
                 
            public final int compareTo(ShreddedReader other) {
                ShreddedBuffer otherBuffer = other.getBuffer();
                
                if (buffer.isAtEnd() && otherBuffer.isAtEnd()) {
                    return 0;                 
                } else if (buffer.isAtEnd()) {
                    return -1;
                } else if (otherBuffer.isAtEnd()) {
                    return 1;
                }
                                   
                int result = 0;
                do {
                    result = + CmpUtil.compare(buffer.getIndex(), otherBuffer.getIndex());
                    if(result != 0) break;
                    result = + CmpUtil.compare(buffer.getBegId(), otherBuffer.getBegId());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final IndexSplit read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                IndexSplit result = new IndexSplit();
                
                result.index = buffer.getIndex();
                result.begId = buffer.getBegId();
                result.endId = buffer.getEndId();
                
                buffer.incrementTuple();
                buffer.autoIncrementIndex();
                buffer.autoIncrementBegId();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateIndexCount - tupleCount > 0) {
                            buffer.indexs.add(last.index);
                            buffer.indexTupleIdx.add((int) (updateIndexCount - tupleCount));
                        }                              
                        if(updateBegIdCount - tupleCount > 0) {
                            buffer.begIds.add(last.begId);
                            buffer.begIdTupleIdx.add((int) (updateBegIdCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateBegId();
                        buffer.processTuple(input.readLong());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateIndex() throws IOException {
                if (updateIndexCount > tupleCount)
                    return;
                     
                last.index = input.readString();
                updateIndexCount = tupleCount + input.readInt();
                                      
                buffer.processIndex(last.index);
            }
            public final void updateBegId() throws IOException {
                if (updateBegIdCount > tupleCount)
                    return;
                     
                updateIndex();
                last.begId = input.readLong();
                updateBegIdCount = tupleCount + input.readInt();
                                      
                buffer.processBegId(last.begId);
            }

            public void run() throws IOException {
                while (true) {
                    fill();
                    
                    if (buffer.isAtEnd())
                        break;
                    
                    buffer.copyUntil(null, processor);
                }      
                processor.close();
            }
            
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof IndexSplit.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((IndexSplit.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<IndexSplit>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<IndexSplit> getOutputClass() {
                return IndexSplit.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            IndexSplit last = new IndexSplit();
            boolean indexProcess = true;
            boolean begIdProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processIndex(String index) throws IOException {  
                if (indexProcess || CmpUtil.compare(index, last.index) != 0) {
                    last.index = index;
                    processor.processIndex(index);
            resetBegId();
                    indexProcess = false;
                }
            }
            public void processBegId(long begId) throws IOException {  
                if (begIdProcess || CmpUtil.compare(begId, last.begId) != 0) {
                    last.begId = begId;
                    processor.processBegId(begId);
                    begIdProcess = false;
                }
            }  
            
            public void resetIndex() {
                 indexProcess = true;
            resetBegId();
            }                                                
            public void resetBegId() {
                 begIdProcess = true;
            }                                                
                               
            public void processTuple(long endId) throws IOException {
                processor.processTuple(endId);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            IndexSplit last = new IndexSplit();
            public org.lemurproject.galago.tupleflow.Processor<IndexSplit> processor;                               
            
            public TupleUnshredder(IndexSplit.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<IndexSplit> processor) {
                this.processor = processor;
            }
            
            public IndexSplit clone(IndexSplit object) {
                IndexSplit result = new IndexSplit();
                if (object == null) return result;
                result.index = object.index; 
                result.begId = object.begId; 
                result.endId = object.endId; 
                return result;
            }                 
            
            public void processIndex(String index) throws IOException {
                last.index = index;
            }   
                
            public void processBegId(long begId) throws IOException {
                last.begId = begId;
            }   
                
            
            public void processTuple(long endId) throws IOException {
                last.endId = endId;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            IndexSplit last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public IndexSplit clone(IndexSplit object) {
                IndexSplit result = new IndexSplit();
                if (object == null) return result;
                result.index = object.index; 
                result.begId = object.begId; 
                result.endId = object.endId; 
                return result;
            }                 
            
            public void process(IndexSplit object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.index, object.index) != 0 || processAll) { processor.processIndex(object.index); processAll = true; }
                if(last == null || CmpUtil.compare(last.begId, object.begId) != 0 || processAll) { processor.processBegId(object.begId); processAll = true; }
                processor.processTuple(object.endId);                                         
                last = object;
            }
                          
            public Class<IndexSplit> getInputClass() {
                return IndexSplit.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    