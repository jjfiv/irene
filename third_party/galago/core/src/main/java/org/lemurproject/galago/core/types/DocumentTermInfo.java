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
 * Tupleflow-Typebuilder automatically-generated class: DocumentTermInfo.
 */
@SuppressWarnings({"unused","unchecked"})
public final class DocumentTermInfo implements Type<DocumentTermInfo> {
    public long docid;
    public String term;
    public int begPos;
    public int endPos;
    public int begOffs;
    public int endOffs; 
    
    /** default constructor makes most fields null */
    public DocumentTermInfo() {}
    /** additional constructor takes all fields explicitly */
    public DocumentTermInfo(long docid, String term, int begPos, int endPos, int begOffs, int endOffs) {
        this.docid = docid;
        this.term = term;
        this.begPos = begPos;
        this.endPos = endPos;
        this.begOffs = begOffs;
        this.endOffs = endOffs;
    }  
    
    public String toString() {
            return String.format("%d,%s,%d,%d,%d,%d",
                                   docid, term, begPos, endPos, begOffs, endOffs);
    } 

    public Order<DocumentTermInfo> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+docid", "+term", "+begPos" })) {
            return new DocidTermBegPosOrder();
        }
        if (Arrays.equals(spec, new String[] { "+docid" })) {
            return new DocidOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo> {
        public void process(DocumentTermInfo object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class DocidTermBegPosOrder implements Order<DocumentTermInfo> {
        public int hash(DocumentTermInfo object) {
            int h = 0;
            h += CmpUtil.hash(object.docid);
            h += CmpUtil.hash(object.term);
            h += CmpUtil.hash(object.begPos);
            return h;
        } 
        public Comparator<DocumentTermInfo> greaterThan() {
            return new Comparator<DocumentTermInfo>() {
                public int compare(DocumentTermInfo one, DocumentTermInfo two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.docid, two.docid);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.term, two.term);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.begPos, two.begPos);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentTermInfo> lessThan() {
            return new Comparator<DocumentTermInfo>() {
                public int compare(DocumentTermInfo one, DocumentTermInfo two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.docid, two.docid);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.term, two.term);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.begPos, two.begPos);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentTermInfo> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentTermInfo> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentTermInfo> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentTermInfo > {
            DocumentTermInfo last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentTermInfo object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.docid, last.docid)) { processAll = true; shreddedWriter.processDocid(object.docid); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.term, last.term)) { processAll = true; shreddedWriter.processTerm(object.term); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.begPos, last.begPos)) { processAll = true; shreddedWriter.processBegPos(object.begPos); }
               shreddedWriter.processTuple(object.endPos, object.begOffs, object.endOffs);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentTermInfo> getInputClass() {
                return DocumentTermInfo.class;
            }
        } 
        public ReaderSource<DocumentTermInfo> orderedCombiner(Collection<TypeReader<DocumentTermInfo>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentTermInfo> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentTermInfo clone(DocumentTermInfo object) {
            DocumentTermInfo result = new DocumentTermInfo();
            if (object == null) return result;
            result.docid = object.docid; 
            result.term = object.term; 
            result.begPos = object.begPos; 
            result.endPos = object.endPos; 
            result.begOffs = object.begOffs; 
            result.endOffs = object.endOffs; 
            return result;
        }                 
        public Class<DocumentTermInfo> getOrderedClass() {
            return DocumentTermInfo.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+docid", "+term", "+begPos"};
        }

        public static String[] getSpec() {
            return new String[] {"+docid", "+term", "+begPos"};
        }
        public static String getSpecString() {
            return "+docid +term +begPos";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processDocid(long docid) throws IOException;
            public void processTerm(String term) throws IOException;
            public void processBegPos(int begPos) throws IOException;
            public void processTuple(int endPos, int begOffs, int endOffs) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            long lastDocid;
            String lastTerm;
            int lastBegPos;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processDocid(long docid) {
                lastDocid = docid;
                buffer.processDocid(docid);
            }
            public void processTerm(String term) {
                lastTerm = term;
                buffer.processTerm(term);
            }
            public void processBegPos(int begPos) {
                lastBegPos = begPos;
                buffer.processBegPos(begPos);
            }
            public final void processTuple(int endPos, int begOffs, int endOffs) throws IOException {
                if (lastFlush) {
                    if(buffer.docids.size() == 0) buffer.processDocid(lastDocid);
                    if(buffer.terms.size() == 0) buffer.processTerm(lastTerm);
                    if(buffer.begPoss.size() == 0) buffer.processBegPos(lastBegPos);
                    lastFlush = false;
                }
                buffer.processTuple(endPos, begOffs, endOffs);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeInt(buffer.getEndPos());
                    output.writeInt(buffer.getBegOffs());
                    output.writeInt(buffer.getEndOffs());
                    buffer.incrementTuple();
                }
            }  
            public final void flushDocid(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getDocidEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getDocid());
                    output.writeInt(count);
                    buffer.incrementDocid();
                      
                    flushTerm(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushTerm(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getTermEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getTerm());
                    output.writeInt(count);
                    buffer.incrementTerm();
                      
                    flushBegPos(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushBegPos(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getBegPosEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeInt(buffer.getBegPos());
                    output.writeInt(count);
                    buffer.incrementBegPos();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushDocid(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TLongArrayList docids = new TLongArrayList();
            ArrayList<String> terms = new ArrayList<String>();
            TIntArrayList begPoss = new TIntArrayList();
            TIntArrayList docidTupleIdx = new TIntArrayList();
            TIntArrayList termTupleIdx = new TIntArrayList();
            TIntArrayList begPosTupleIdx = new TIntArrayList();
            int docidReadIdx = 0;
            int termReadIdx = 0;
            int begPosReadIdx = 0;
                            
            int[] endPoss;
            int[] begOffss;
            int[] endOffss;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                endPoss = new int[batchSize];
                begOffss = new int[batchSize];
                endOffss = new int[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDocid(long docid) {
                docids.add(docid);
                docidTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTerm(String term) {
                terms.add(term);
                termTupleIdx.add(writeTupleIndex);
            }                                      
            public void processBegPos(int begPos) {
                begPoss.add(begPos);
                begPosTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(int endPos, int begOffs, int endOffs) {
                assert docids.size() > 0;
                assert terms.size() > 0;
                assert begPoss.size() > 0;
                endPoss[writeTupleIndex] = endPos;
                begOffss[writeTupleIndex] = begOffs;
                endOffss[writeTupleIndex] = endOffs;
                writeTupleIndex++;
            }
            public void resetData() {
                docids.clear();
                terms.clear();
                begPoss.clear();
                docidTupleIdx.clear();
                termTupleIdx.clear();
                begPosTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                docidReadIdx = 0;
                termReadIdx = 0;
                begPosReadIdx = 0;
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
            public void incrementDocid() {
                docidReadIdx++;  
            }                                                                                              

            public void autoIncrementDocid() {
                while (readTupleIndex >= getDocidEndIndex() && readTupleIndex < writeTupleIndex)
                    docidReadIdx++;
            }                 
            public void incrementTerm() {
                termReadIdx++;  
            }                                                                                              

            public void autoIncrementTerm() {
                while (readTupleIndex >= getTermEndIndex() && readTupleIndex < writeTupleIndex)
                    termReadIdx++;
            }                 
            public void incrementBegPos() {
                begPosReadIdx++;  
            }                                                                                              

            public void autoIncrementBegPos() {
                while (readTupleIndex >= getBegPosEndIndex() && readTupleIndex < writeTupleIndex)
                    begPosReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getDocidEndIndex() {
                if ((docidReadIdx+1) >= docidTupleIdx.size())
                    return writeTupleIndex;
                return docidTupleIdx.get(docidReadIdx+1);
            }

            public int getTermEndIndex() {
                if ((termReadIdx+1) >= termTupleIdx.size())
                    return writeTupleIndex;
                return termTupleIdx.get(termReadIdx+1);
            }

            public int getBegPosEndIndex() {
                if ((begPosReadIdx+1) >= begPosTupleIdx.size())
                    return writeTupleIndex;
                return begPosTupleIdx.get(begPosReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public long getDocid() {
                assert readTupleIndex < writeTupleIndex;
                assert docidReadIdx < docids.size();
                
                return docids.get(docidReadIdx);
            }
            public String getTerm() {
                assert readTupleIndex < writeTupleIndex;
                assert termReadIdx < terms.size();
                
                return terms.get(termReadIdx);
            }
            public int getBegPos() {
                assert readTupleIndex < writeTupleIndex;
                assert begPosReadIdx < begPoss.size();
                
                return begPoss.get(begPosReadIdx);
            }
            public int getEndPos() {
                assert readTupleIndex < writeTupleIndex;
                return endPoss[readTupleIndex];
            }                                         
            public int getBegOffs() {
                assert readTupleIndex < writeTupleIndex;
                return begOffss[readTupleIndex];
            }                                         
            public int getEndOffs() {
                assert readTupleIndex < writeTupleIndex;
                return endOffss[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getEndPos(), getBegOffs(), getEndOffs());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexDocid(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processDocid(getDocid());
                    assert getDocidEndIndex() <= endIndex;
                    copyUntilIndexTerm(getDocidEndIndex(), output);
                    incrementDocid();
                }
            } 
            public void copyUntilIndexTerm(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processTerm(getTerm());
                    assert getTermEndIndex() <= endIndex;
                    copyUntilIndexBegPos(getTermEndIndex(), output);
                    incrementTerm();
                }
            } 
            public void copyUntilIndexBegPos(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processBegPos(getBegPos());
                    assert getBegPosEndIndex() <= endIndex;
                    copyTuples(getBegPosEndIndex(), output);
                    incrementBegPos();
                }
            }  
            public void copyUntilDocid(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getDocid(), other.getDocid());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processDocid(getDocid());
                                      
                        if (c < 0) {
                            copyUntilIndexTerm(getDocidEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilTerm(other, output);
                            autoIncrementDocid();
                            break;
                        }
                    } else {
                        output.processDocid(getDocid());
                        copyUntilIndexTerm(getDocidEndIndex(), output);
                    }
                    incrementDocid();  
                    
               
                }
            }
            public void copyUntilTerm(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getTerm(), other.getTerm());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processTerm(getTerm());
                                      
                        if (c < 0) {
                            copyUntilIndexBegPos(getTermEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilBegPos(other, output);
                            autoIncrementTerm();
                            break;
                        }
                    } else {
                        output.processTerm(getTerm());
                        copyUntilIndexBegPos(getTermEndIndex(), output);
                    }
                    incrementTerm();  
                    
                    if (getDocidEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntilBegPos(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getBegPos(), other.getBegPos());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processBegPos(getBegPos());
                                      
                        copyTuples(getBegPosEndIndex(), output);
                    } else {
                        output.processBegPos(getBegPos());
                        copyTuples(getBegPosEndIndex(), output);
                    }
                    incrementBegPos();  
                    
                    if (getTermEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilDocid(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentTermInfo>, ShreddedSource {
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
                } else if (processor instanceof DocumentTermInfo.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentTermInfo.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentTermInfo> getOutputClass() {
                return DocumentTermInfo.class;
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

            public DocumentTermInfo read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentTermInfo result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentTermInfo>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentTermInfo last = new DocumentTermInfo();         
            long updateDocidCount = -1;
            long updateTermCount = -1;
            long updateBegPosCount = -1;
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
                    result = + CmpUtil.compare(buffer.getDocid(), otherBuffer.getDocid());
                    if(result != 0) break;
                    result = + CmpUtil.compare(buffer.getTerm(), otherBuffer.getTerm());
                    if(result != 0) break;
                    result = + CmpUtil.compare(buffer.getBegPos(), otherBuffer.getBegPos());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentTermInfo read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentTermInfo result = new DocumentTermInfo();
                
                result.docid = buffer.getDocid();
                result.term = buffer.getTerm();
                result.begPos = buffer.getBegPos();
                result.endPos = buffer.getEndPos();
                result.begOffs = buffer.getBegOffs();
                result.endOffs = buffer.getEndOffs();
                
                buffer.incrementTuple();
                buffer.autoIncrementDocid();
                buffer.autoIncrementTerm();
                buffer.autoIncrementBegPos();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateDocidCount - tupleCount > 0) {
                            buffer.docids.add(last.docid);
                            buffer.docidTupleIdx.add((int) (updateDocidCount - tupleCount));
                        }                              
                        if(updateTermCount - tupleCount > 0) {
                            buffer.terms.add(last.term);
                            buffer.termTupleIdx.add((int) (updateTermCount - tupleCount));
                        }                              
                        if(updateBegPosCount - tupleCount > 0) {
                            buffer.begPoss.add(last.begPos);
                            buffer.begPosTupleIdx.add((int) (updateBegPosCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateBegPos();
                        buffer.processTuple(input.readInt(), input.readInt(), input.readInt());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateDocid() throws IOException {
                if (updateDocidCount > tupleCount)
                    return;
                     
                last.docid = input.readLong();
                updateDocidCount = tupleCount + input.readInt();
                                      
                buffer.processDocid(last.docid);
            }
            public final void updateTerm() throws IOException {
                if (updateTermCount > tupleCount)
                    return;
                     
                updateDocid();
                last.term = input.readString();
                updateTermCount = tupleCount + input.readInt();
                                      
                buffer.processTerm(last.term);
            }
            public final void updateBegPos() throws IOException {
                if (updateBegPosCount > tupleCount)
                    return;
                     
                updateTerm();
                last.begPos = input.readInt();
                updateBegPosCount = tupleCount + input.readInt();
                                      
                buffer.processBegPos(last.begPos);
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
                } else if (processor instanceof DocumentTermInfo.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentTermInfo.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentTermInfo> getOutputClass() {
                return DocumentTermInfo.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentTermInfo last = new DocumentTermInfo();
            boolean docidProcess = true;
            boolean termProcess = true;
            boolean begPosProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processDocid(long docid) throws IOException {  
                if (docidProcess || CmpUtil.compare(docid, last.docid) != 0) {
                    last.docid = docid;
                    processor.processDocid(docid);
            resetTerm();
                    docidProcess = false;
                }
            }
            public void processTerm(String term) throws IOException {  
                if (termProcess || CmpUtil.compare(term, last.term) != 0) {
                    last.term = term;
                    processor.processTerm(term);
            resetBegPos();
                    termProcess = false;
                }
            }
            public void processBegPos(int begPos) throws IOException {  
                if (begPosProcess || CmpUtil.compare(begPos, last.begPos) != 0) {
                    last.begPos = begPos;
                    processor.processBegPos(begPos);
                    begPosProcess = false;
                }
            }  
            
            public void resetDocid() {
                 docidProcess = true;
            resetTerm();
            }                                                
            public void resetTerm() {
                 termProcess = true;
            resetBegPos();
            }                                                
            public void resetBegPos() {
                 begPosProcess = true;
            }                                                
                               
            public void processTuple(int endPos, int begOffs, int endOffs) throws IOException {
                processor.processTuple(endPos, begOffs, endOffs);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentTermInfo last = new DocumentTermInfo();
            public org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo> processor;                               
            
            public TupleUnshredder(DocumentTermInfo.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo> processor) {
                this.processor = processor;
            }
            
            public DocumentTermInfo clone(DocumentTermInfo object) {
                DocumentTermInfo result = new DocumentTermInfo();
                if (object == null) return result;
                result.docid = object.docid; 
                result.term = object.term; 
                result.begPos = object.begPos; 
                result.endPos = object.endPos; 
                result.begOffs = object.begOffs; 
                result.endOffs = object.endOffs; 
                return result;
            }                 
            
            public void processDocid(long docid) throws IOException {
                last.docid = docid;
            }   
                
            public void processTerm(String term) throws IOException {
                last.term = term;
            }   
                
            public void processBegPos(int begPos) throws IOException {
                last.begPos = begPos;
            }   
                
            
            public void processTuple(int endPos, int begOffs, int endOffs) throws IOException {
                last.endPos = endPos;
                last.begOffs = begOffs;
                last.endOffs = endOffs;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            DocumentTermInfo last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentTermInfo clone(DocumentTermInfo object) {
                DocumentTermInfo result = new DocumentTermInfo();
                if (object == null) return result;
                result.docid = object.docid; 
                result.term = object.term; 
                result.begPos = object.begPos; 
                result.endPos = object.endPos; 
                result.begOffs = object.begOffs; 
                result.endOffs = object.endOffs; 
                return result;
            }                 
            
            public void process(DocumentTermInfo object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.docid, object.docid) != 0 || processAll) { processor.processDocid(object.docid); processAll = true; }
                if(last == null || CmpUtil.compare(last.term, object.term) != 0 || processAll) { processor.processTerm(object.term); processAll = true; }
                if(last == null || CmpUtil.compare(last.begPos, object.begPos) != 0 || processAll) { processor.processBegPos(object.begPos); processAll = true; }
                processor.processTuple(object.endPos, object.begOffs, object.endOffs);                                         
                last = object;
            }
                          
            public Class<DocumentTermInfo> getInputClass() {
                return DocumentTermInfo.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class DocidOrder implements Order<DocumentTermInfo> {
        public int hash(DocumentTermInfo object) {
            int h = 0;
            h += CmpUtil.hash(object.docid);
            return h;
        } 
        public Comparator<DocumentTermInfo> greaterThan() {
            return new Comparator<DocumentTermInfo>() {
                public int compare(DocumentTermInfo one, DocumentTermInfo two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.docid, two.docid);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentTermInfo> lessThan() {
            return new Comparator<DocumentTermInfo>() {
                public int compare(DocumentTermInfo one, DocumentTermInfo two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.docid, two.docid);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentTermInfo> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentTermInfo> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentTermInfo> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentTermInfo > {
            DocumentTermInfo last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentTermInfo object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.docid, last.docid)) { processAll = true; shreddedWriter.processDocid(object.docid); }
               shreddedWriter.processTuple(object.term, object.begPos, object.endPos, object.begOffs, object.endOffs);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentTermInfo> getInputClass() {
                return DocumentTermInfo.class;
            }
        } 
        public ReaderSource<DocumentTermInfo> orderedCombiner(Collection<TypeReader<DocumentTermInfo>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentTermInfo> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentTermInfo clone(DocumentTermInfo object) {
            DocumentTermInfo result = new DocumentTermInfo();
            if (object == null) return result;
            result.docid = object.docid; 
            result.term = object.term; 
            result.begPos = object.begPos; 
            result.endPos = object.endPos; 
            result.begOffs = object.begOffs; 
            result.endOffs = object.endOffs; 
            return result;
        }                 
        public Class<DocumentTermInfo> getOrderedClass() {
            return DocumentTermInfo.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+docid"};
        }

        public static String[] getSpec() {
            return new String[] {"+docid"};
        }
        public static String getSpecString() {
            return "+docid";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processDocid(long docid) throws IOException;
            public void processTuple(String term, int begPos, int endPos, int begOffs, int endOffs) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            long lastDocid;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processDocid(long docid) {
                lastDocid = docid;
                buffer.processDocid(docid);
            }
            public final void processTuple(String term, int begPos, int endPos, int begOffs, int endOffs) throws IOException {
                if (lastFlush) {
                    if(buffer.docids.size() == 0) buffer.processDocid(lastDocid);
                    lastFlush = false;
                }
                buffer.processTuple(term, begPos, endPos, begOffs, endOffs);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getTerm());
                    output.writeInt(buffer.getBegPos());
                    output.writeInt(buffer.getEndPos());
                    output.writeInt(buffer.getBegOffs());
                    output.writeInt(buffer.getEndOffs());
                    buffer.incrementTuple();
                }
            }  
            public final void flushDocid(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getDocidEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getDocid());
                    output.writeInt(count);
                    buffer.incrementDocid();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushDocid(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TLongArrayList docids = new TLongArrayList();
            TIntArrayList docidTupleIdx = new TIntArrayList();
            int docidReadIdx = 0;
                            
            String[] terms;
            int[] begPoss;
            int[] endPoss;
            int[] begOffss;
            int[] endOffss;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                terms = new String[batchSize];
                begPoss = new int[batchSize];
                endPoss = new int[batchSize];
                begOffss = new int[batchSize];
                endOffss = new int[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDocid(long docid) {
                docids.add(docid);
                docidTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String term, int begPos, int endPos, int begOffs, int endOffs) {
                assert docids.size() > 0;
                terms[writeTupleIndex] = term;
                begPoss[writeTupleIndex] = begPos;
                endPoss[writeTupleIndex] = endPos;
                begOffss[writeTupleIndex] = begOffs;
                endOffss[writeTupleIndex] = endOffs;
                writeTupleIndex++;
            }
            public void resetData() {
                docids.clear();
                docidTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                docidReadIdx = 0;
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
            public void incrementDocid() {
                docidReadIdx++;  
            }                                                                                              

            public void autoIncrementDocid() {
                while (readTupleIndex >= getDocidEndIndex() && readTupleIndex < writeTupleIndex)
                    docidReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getDocidEndIndex() {
                if ((docidReadIdx+1) >= docidTupleIdx.size())
                    return writeTupleIndex;
                return docidTupleIdx.get(docidReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public long getDocid() {
                assert readTupleIndex < writeTupleIndex;
                assert docidReadIdx < docids.size();
                
                return docids.get(docidReadIdx);
            }
            public String getTerm() {
                assert readTupleIndex < writeTupleIndex;
                return terms[readTupleIndex];
            }                                         
            public int getBegPos() {
                assert readTupleIndex < writeTupleIndex;
                return begPoss[readTupleIndex];
            }                                         
            public int getEndPos() {
                assert readTupleIndex < writeTupleIndex;
                return endPoss[readTupleIndex];
            }                                         
            public int getBegOffs() {
                assert readTupleIndex < writeTupleIndex;
                return begOffss[readTupleIndex];
            }                                         
            public int getEndOffs() {
                assert readTupleIndex < writeTupleIndex;
                return endOffss[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getTerm(), getBegPos(), getEndPos(), getBegOffs(), getEndOffs());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexDocid(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processDocid(getDocid());
                    assert getDocidEndIndex() <= endIndex;
                    copyTuples(getDocidEndIndex(), output);
                    incrementDocid();
                }
            }  
            public void copyUntilDocid(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getDocid(), other.getDocid());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processDocid(getDocid());
                                      
                        copyTuples(getDocidEndIndex(), output);
                    } else {
                        output.processDocid(getDocid());
                        copyTuples(getDocidEndIndex(), output);
                    }
                    incrementDocid();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilDocid(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentTermInfo>, ShreddedSource {
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
                } else if (processor instanceof DocumentTermInfo.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentTermInfo.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentTermInfo> getOutputClass() {
                return DocumentTermInfo.class;
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

            public DocumentTermInfo read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentTermInfo result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentTermInfo>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentTermInfo last = new DocumentTermInfo();         
            long updateDocidCount = -1;
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
                    result = + CmpUtil.compare(buffer.getDocid(), otherBuffer.getDocid());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentTermInfo read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentTermInfo result = new DocumentTermInfo();
                
                result.docid = buffer.getDocid();
                result.term = buffer.getTerm();
                result.begPos = buffer.getBegPos();
                result.endPos = buffer.getEndPos();
                result.begOffs = buffer.getBegOffs();
                result.endOffs = buffer.getEndOffs();
                
                buffer.incrementTuple();
                buffer.autoIncrementDocid();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateDocidCount - tupleCount > 0) {
                            buffer.docids.add(last.docid);
                            buffer.docidTupleIdx.add((int) (updateDocidCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateDocid();
                        buffer.processTuple(input.readString(), input.readInt(), input.readInt(), input.readInt(), input.readInt());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateDocid() throws IOException {
                if (updateDocidCount > tupleCount)
                    return;
                     
                last.docid = input.readLong();
                updateDocidCount = tupleCount + input.readInt();
                                      
                buffer.processDocid(last.docid);
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
                } else if (processor instanceof DocumentTermInfo.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentTermInfo.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentTermInfo> getOutputClass() {
                return DocumentTermInfo.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentTermInfo last = new DocumentTermInfo();
            boolean docidProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processDocid(long docid) throws IOException {  
                if (docidProcess || CmpUtil.compare(docid, last.docid) != 0) {
                    last.docid = docid;
                    processor.processDocid(docid);
                    docidProcess = false;
                }
            }  
            
            public void resetDocid() {
                 docidProcess = true;
            }                                                
                               
            public void processTuple(String term, int begPos, int endPos, int begOffs, int endOffs) throws IOException {
                processor.processTuple(term, begPos, endPos, begOffs, endOffs);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentTermInfo last = new DocumentTermInfo();
            public org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo> processor;                               
            
            public TupleUnshredder(DocumentTermInfo.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentTermInfo> processor) {
                this.processor = processor;
            }
            
            public DocumentTermInfo clone(DocumentTermInfo object) {
                DocumentTermInfo result = new DocumentTermInfo();
                if (object == null) return result;
                result.docid = object.docid; 
                result.term = object.term; 
                result.begPos = object.begPos; 
                result.endPos = object.endPos; 
                result.begOffs = object.begOffs; 
                result.endOffs = object.endOffs; 
                return result;
            }                 
            
            public void processDocid(long docid) throws IOException {
                last.docid = docid;
            }   
                
            
            public void processTuple(String term, int begPos, int endPos, int begOffs, int endOffs) throws IOException {
                last.term = term;
                last.begPos = begPos;
                last.endPos = endPos;
                last.begOffs = begOffs;
                last.endOffs = endOffs;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            DocumentTermInfo last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentTermInfo clone(DocumentTermInfo object) {
                DocumentTermInfo result = new DocumentTermInfo();
                if (object == null) return result;
                result.docid = object.docid; 
                result.term = object.term; 
                result.begPos = object.begPos; 
                result.endPos = object.endPos; 
                result.begOffs = object.begOffs; 
                result.endOffs = object.endOffs; 
                return result;
            }                 
            
            public void process(DocumentTermInfo object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.docid, object.docid) != 0 || processAll) { processor.processDocid(object.docid); processAll = true; }
                processor.processTuple(object.term, object.begPos, object.endPos, object.begOffs, object.endOffs);                                         
                last = object;
            }
                          
            public Class<DocumentTermInfo> getInputClass() {
                return DocumentTermInfo.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    