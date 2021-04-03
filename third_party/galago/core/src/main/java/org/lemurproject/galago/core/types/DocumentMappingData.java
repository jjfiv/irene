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
 * Tupleflow-Typebuilder automatically-generated class: DocumentMappingData.
 */
@SuppressWarnings({"unused","unchecked"})
public final class DocumentMappingData implements Type<DocumentMappingData> {
    public int indexId;
    public long docNumIncrement; 
    
    /** default constructor makes most fields null */
    public DocumentMappingData() {}
    /** additional constructor takes all fields explicitly */
    public DocumentMappingData(int indexId, long docNumIncrement) {
        this.indexId = indexId;
        this.docNumIncrement = docNumIncrement;
    }  
    
    public String toString() {
            return String.format("%d,%d",
                                   indexId, docNumIncrement);
    } 

    public Order<DocumentMappingData> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+indexId" })) {
            return new IndexIdOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<DocumentMappingData> {
        public void process(DocumentMappingData object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class IndexIdOrder implements Order<DocumentMappingData> {
        public int hash(DocumentMappingData object) {
            int h = 0;
            h += CmpUtil.hash(object.indexId);
            return h;
        } 
        public Comparator<DocumentMappingData> greaterThan() {
            return new Comparator<DocumentMappingData>() {
                public int compare(DocumentMappingData one, DocumentMappingData two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.indexId, two.indexId);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentMappingData> lessThan() {
            return new Comparator<DocumentMappingData>() {
                public int compare(DocumentMappingData one, DocumentMappingData two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.indexId, two.indexId);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentMappingData> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentMappingData> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentMappingData> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentMappingData > {
            DocumentMappingData last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentMappingData object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.indexId, last.indexId)) { processAll = true; shreddedWriter.processIndexId(object.indexId); }
               shreddedWriter.processTuple(object.docNumIncrement);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentMappingData> getInputClass() {
                return DocumentMappingData.class;
            }
        } 
        public ReaderSource<DocumentMappingData> orderedCombiner(Collection<TypeReader<DocumentMappingData>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentMappingData> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentMappingData clone(DocumentMappingData object) {
            DocumentMappingData result = new DocumentMappingData();
            if (object == null) return result;
            result.indexId = object.indexId; 
            result.docNumIncrement = object.docNumIncrement; 
            return result;
        }                 
        public Class<DocumentMappingData> getOrderedClass() {
            return DocumentMappingData.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+indexId"};
        }

        public static String[] getSpec() {
            return new String[] {"+indexId"};
        }
        public static String getSpecString() {
            return "+indexId";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processIndexId(int indexId) throws IOException;
            public void processTuple(long docNumIncrement) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            int lastIndexId;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processIndexId(int indexId) {
                lastIndexId = indexId;
                buffer.processIndexId(indexId);
            }
            public final void processTuple(long docNumIncrement) throws IOException {
                if (lastFlush) {
                    if(buffer.indexIds.size() == 0) buffer.processIndexId(lastIndexId);
                    lastFlush = false;
                }
                buffer.processTuple(docNumIncrement);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeLong(buffer.getDocNumIncrement());
                    buffer.incrementTuple();
                }
            }  
            public final void flushIndexId(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getIndexIdEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeInt(buffer.getIndexId());
                    output.writeInt(count);
                    buffer.incrementIndexId();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushIndexId(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TIntArrayList indexIds = new TIntArrayList();
            TIntArrayList indexIdTupleIdx = new TIntArrayList();
            int indexIdReadIdx = 0;
                            
            long[] docNumIncrements;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                docNumIncrements = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processIndexId(int indexId) {
                indexIds.add(indexId);
                indexIdTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(long docNumIncrement) {
                assert indexIds.size() > 0;
                docNumIncrements[writeTupleIndex] = docNumIncrement;
                writeTupleIndex++;
            }
            public void resetData() {
                indexIds.clear();
                indexIdTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                indexIdReadIdx = 0;
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
            public void incrementIndexId() {
                indexIdReadIdx++;  
            }                                                                                              

            public void autoIncrementIndexId() {
                while (readTupleIndex >= getIndexIdEndIndex() && readTupleIndex < writeTupleIndex)
                    indexIdReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getIndexIdEndIndex() {
                if ((indexIdReadIdx+1) >= indexIdTupleIdx.size())
                    return writeTupleIndex;
                return indexIdTupleIdx.get(indexIdReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public int getIndexId() {
                assert readTupleIndex < writeTupleIndex;
                assert indexIdReadIdx < indexIds.size();
                
                return indexIds.get(indexIdReadIdx);
            }
            public long getDocNumIncrement() {
                assert readTupleIndex < writeTupleIndex;
                return docNumIncrements[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getDocNumIncrement());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexIndexId(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processIndexId(getIndexId());
                    assert getIndexIdEndIndex() <= endIndex;
                    copyTuples(getIndexIdEndIndex(), output);
                    incrementIndexId();
                }
            }  
            public void copyUntilIndexId(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getIndexId(), other.getIndexId());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processIndexId(getIndexId());
                                      
                        copyTuples(getIndexIdEndIndex(), output);
                    } else {
                        output.processIndexId(getIndexId());
                        copyTuples(getIndexIdEndIndex(), output);
                    }
                    incrementIndexId();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilIndexId(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentMappingData>, ShreddedSource {
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
                } else if (processor instanceof DocumentMappingData.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentMappingData.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentMappingData>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentMappingData> getOutputClass() {
                return DocumentMappingData.class;
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

            public DocumentMappingData read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentMappingData result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentMappingData>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentMappingData last = new DocumentMappingData();         
            long updateIndexIdCount = -1;
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
                    result = + CmpUtil.compare(buffer.getIndexId(), otherBuffer.getIndexId());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentMappingData read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentMappingData result = new DocumentMappingData();
                
                result.indexId = buffer.getIndexId();
                result.docNumIncrement = buffer.getDocNumIncrement();
                
                buffer.incrementTuple();
                buffer.autoIncrementIndexId();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateIndexIdCount - tupleCount > 0) {
                            buffer.indexIds.add(last.indexId);
                            buffer.indexIdTupleIdx.add((int) (updateIndexIdCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateIndexId();
                        buffer.processTuple(input.readLong());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateIndexId() throws IOException {
                if (updateIndexIdCount > tupleCount)
                    return;
                     
                last.indexId = input.readInt();
                updateIndexIdCount = tupleCount + input.readInt();
                                      
                buffer.processIndexId(last.indexId);
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
                } else if (processor instanceof DocumentMappingData.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentMappingData.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentMappingData>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentMappingData> getOutputClass() {
                return DocumentMappingData.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentMappingData last = new DocumentMappingData();
            boolean indexIdProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processIndexId(int indexId) throws IOException {  
                if (indexIdProcess || CmpUtil.compare(indexId, last.indexId) != 0) {
                    last.indexId = indexId;
                    processor.processIndexId(indexId);
                    indexIdProcess = false;
                }
            }  
            
            public void resetIndexId() {
                 indexIdProcess = true;
            }                                                
                               
            public void processTuple(long docNumIncrement) throws IOException {
                processor.processTuple(docNumIncrement);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentMappingData last = new DocumentMappingData();
            public org.lemurproject.galago.tupleflow.Processor<DocumentMappingData> processor;                               
            
            public TupleUnshredder(DocumentMappingData.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentMappingData> processor) {
                this.processor = processor;
            }
            
            public DocumentMappingData clone(DocumentMappingData object) {
                DocumentMappingData result = new DocumentMappingData();
                if (object == null) return result;
                result.indexId = object.indexId; 
                result.docNumIncrement = object.docNumIncrement; 
                return result;
            }                 
            
            public void processIndexId(int indexId) throws IOException {
                last.indexId = indexId;
            }   
                
            
            public void processTuple(long docNumIncrement) throws IOException {
                last.docNumIncrement = docNumIncrement;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            DocumentMappingData last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentMappingData clone(DocumentMappingData object) {
                DocumentMappingData result = new DocumentMappingData();
                if (object == null) return result;
                result.indexId = object.indexId; 
                result.docNumIncrement = object.docNumIncrement; 
                return result;
            }                 
            
            public void process(DocumentMappingData object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.indexId, object.indexId) != 0 || processAll) { processor.processIndexId(object.indexId); processAll = true; }
                processor.processTuple(object.docNumIncrement);                                         
                last = object;
            }
                          
            public Class<DocumentMappingData> getInputClass() {
                return DocumentMappingData.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    