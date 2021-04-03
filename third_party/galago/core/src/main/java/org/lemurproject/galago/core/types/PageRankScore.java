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
 * Tupleflow-Typebuilder automatically-generated class: PageRankScore.
 */
@SuppressWarnings({"unused","unchecked"})
public final class PageRankScore implements Type<PageRankScore> {
    public String docName;
    public double score; 
    
    /** default constructor makes most fields null */
    public PageRankScore() {}
    /** additional constructor takes all fields explicitly */
    public PageRankScore(String docName, double score) {
        this.docName = docName;
        this.score = score;
    }  
    
    public String toString() {
            return String.format("%s,%g",
                                   docName, score);
    } 

    public Order<PageRankScore> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+docName" })) {
            return new DocNameOrder();
        }
        if (Arrays.equals(spec, new String[] { "+score" })) {
            return new ScoreOrder();
        }
        if (Arrays.equals(spec, new String[] { "-score" })) {
            return new DescScoreOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<PageRankScore> {
        public void process(PageRankScore object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class DocNameOrder implements Order<PageRankScore> {
        public int hash(PageRankScore object) {
            int h = 0;
            h += CmpUtil.hash(object.docName);
            return h;
        } 
        public Comparator<PageRankScore> greaterThan() {
            return new Comparator<PageRankScore>() {
                public int compare(PageRankScore one, PageRankScore two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.docName, two.docName);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<PageRankScore> lessThan() {
            return new Comparator<PageRankScore>() {
                public int compare(PageRankScore one, PageRankScore two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.docName, two.docName);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<PageRankScore> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<PageRankScore> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<PageRankScore> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< PageRankScore > {
            PageRankScore last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(PageRankScore object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.docName, last.docName)) { processAll = true; shreddedWriter.processDocName(object.docName); }
               shreddedWriter.processTuple(object.score);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<PageRankScore> getInputClass() {
                return PageRankScore.class;
            }
        } 
        public ReaderSource<PageRankScore> orderedCombiner(Collection<TypeReader<PageRankScore>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<PageRankScore> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public PageRankScore clone(PageRankScore object) {
            PageRankScore result = new PageRankScore();
            if (object == null) return result;
            result.docName = object.docName; 
            result.score = object.score; 
            return result;
        }                 
        public Class<PageRankScore> getOrderedClass() {
            return PageRankScore.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+docName"};
        }

        public static String[] getSpec() {
            return new String[] {"+docName"};
        }
        public static String getSpecString() {
            return "+docName";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processDocName(String docName) throws IOException;
            public void processTuple(double score) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastDocName;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processDocName(String docName) {
                lastDocName = docName;
                buffer.processDocName(docName);
            }
            public final void processTuple(double score) throws IOException {
                if (lastFlush) {
                    if(buffer.docNames.size() == 0) buffer.processDocName(lastDocName);
                    lastFlush = false;
                }
                buffer.processTuple(score);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeDouble(buffer.getScore());
                    buffer.incrementTuple();
                }
            }  
            public final void flushDocName(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getDocNameEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getDocName());
                    output.writeInt(count);
                    buffer.incrementDocName();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushDocName(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> docNames = new ArrayList<String>();
            TIntArrayList docNameTupleIdx = new TIntArrayList();
            int docNameReadIdx = 0;
                            
            double[] scores;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                scores = new double[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDocName(String docName) {
                docNames.add(docName);
                docNameTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(double score) {
                assert docNames.size() > 0;
                scores[writeTupleIndex] = score;
                writeTupleIndex++;
            }
            public void resetData() {
                docNames.clear();
                docNameTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                docNameReadIdx = 0;
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
            public void incrementDocName() {
                docNameReadIdx++;  
            }                                                                                              

            public void autoIncrementDocName() {
                while (readTupleIndex >= getDocNameEndIndex() && readTupleIndex < writeTupleIndex)
                    docNameReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getDocNameEndIndex() {
                if ((docNameReadIdx+1) >= docNameTupleIdx.size())
                    return writeTupleIndex;
                return docNameTupleIdx.get(docNameReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getDocName() {
                assert readTupleIndex < writeTupleIndex;
                assert docNameReadIdx < docNames.size();
                
                return docNames.get(docNameReadIdx);
            }
            public double getScore() {
                assert readTupleIndex < writeTupleIndex;
                return scores[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getScore());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexDocName(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processDocName(getDocName());
                    assert getDocNameEndIndex() <= endIndex;
                    copyTuples(getDocNameEndIndex(), output);
                    incrementDocName();
                }
            }  
            public void copyUntilDocName(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getDocName(), other.getDocName());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processDocName(getDocName());
                                      
                        copyTuples(getDocNameEndIndex(), output);
                    } else {
                        output.processDocName(getDocName());
                        copyTuples(getDocNameEndIndex(), output);
                    }
                    incrementDocName();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilDocName(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<PageRankScore>, ShreddedSource {
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
                } else if (processor instanceof PageRankScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankScore> getOutputClass() {
                return PageRankScore.class;
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

            public PageRankScore read() throws IOException {
                if (uninitialized)
                    initialize();

                PageRankScore result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<PageRankScore>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            PageRankScore last = new PageRankScore();         
            long updateDocNameCount = -1;
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
                    result = + CmpUtil.compare(buffer.getDocName(), otherBuffer.getDocName());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final PageRankScore read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                PageRankScore result = new PageRankScore();
                
                result.docName = buffer.getDocName();
                result.score = buffer.getScore();
                
                buffer.incrementTuple();
                buffer.autoIncrementDocName();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateDocNameCount - tupleCount > 0) {
                            buffer.docNames.add(last.docName);
                            buffer.docNameTupleIdx.add((int) (updateDocNameCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateDocName();
                        buffer.processTuple(input.readDouble());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateDocName() throws IOException {
                if (updateDocNameCount > tupleCount)
                    return;
                     
                last.docName = input.readString();
                updateDocNameCount = tupleCount + input.readInt();
                                      
                buffer.processDocName(last.docName);
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
                } else if (processor instanceof PageRankScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankScore> getOutputClass() {
                return PageRankScore.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            PageRankScore last = new PageRankScore();
            boolean docNameProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processDocName(String docName) throws IOException {  
                if (docNameProcess || CmpUtil.compare(docName, last.docName) != 0) {
                    last.docName = docName;
                    processor.processDocName(docName);
                    docNameProcess = false;
                }
            }  
            
            public void resetDocName() {
                 docNameProcess = true;
            }                                                
                               
            public void processTuple(double score) throws IOException {
                processor.processTuple(score);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            PageRankScore last = new PageRankScore();
            public org.lemurproject.galago.tupleflow.Processor<PageRankScore> processor;                               
            
            public TupleUnshredder(PageRankScore.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<PageRankScore> processor) {
                this.processor = processor;
            }
            
            public PageRankScore clone(PageRankScore object) {
                PageRankScore result = new PageRankScore();
                if (object == null) return result;
                result.docName = object.docName; 
                result.score = object.score; 
                return result;
            }                 
            
            public void processDocName(String docName) throws IOException {
                last.docName = docName;
            }   
                
            
            public void processTuple(double score) throws IOException {
                last.score = score;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            PageRankScore last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public PageRankScore clone(PageRankScore object) {
                PageRankScore result = new PageRankScore();
                if (object == null) return result;
                result.docName = object.docName; 
                result.score = object.score; 
                return result;
            }                 
            
            public void process(PageRankScore object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.docName, object.docName) != 0 || processAll) { processor.processDocName(object.docName); processAll = true; }
                processor.processTuple(object.score);                                         
                last = object;
            }
                          
            public Class<PageRankScore> getInputClass() {
                return PageRankScore.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class ScoreOrder implements Order<PageRankScore> {
        public int hash(PageRankScore object) {
            int h = 0;
            h += CmpUtil.hash(object.score);
            return h;
        } 
        public Comparator<PageRankScore> greaterThan() {
            return new Comparator<PageRankScore>() {
                public int compare(PageRankScore one, PageRankScore two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.score, two.score);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<PageRankScore> lessThan() {
            return new Comparator<PageRankScore>() {
                public int compare(PageRankScore one, PageRankScore two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.score, two.score);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<PageRankScore> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<PageRankScore> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<PageRankScore> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< PageRankScore > {
            PageRankScore last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(PageRankScore object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.score, last.score)) { processAll = true; shreddedWriter.processScore(object.score); }
               shreddedWriter.processTuple(object.docName);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<PageRankScore> getInputClass() {
                return PageRankScore.class;
            }
        } 
        public ReaderSource<PageRankScore> orderedCombiner(Collection<TypeReader<PageRankScore>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<PageRankScore> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public PageRankScore clone(PageRankScore object) {
            PageRankScore result = new PageRankScore();
            if (object == null) return result;
            result.docName = object.docName; 
            result.score = object.score; 
            return result;
        }                 
        public Class<PageRankScore> getOrderedClass() {
            return PageRankScore.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+score"};
        }

        public static String[] getSpec() {
            return new String[] {"+score"};
        }
        public static String getSpecString() {
            return "+score";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processScore(double score) throws IOException;
            public void processTuple(String docName) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            double lastScore;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processScore(double score) {
                lastScore = score;
                buffer.processScore(score);
            }
            public final void processTuple(String docName) throws IOException {
                if (lastFlush) {
                    if(buffer.scores.size() == 0) buffer.processScore(lastScore);
                    lastFlush = false;
                }
                buffer.processTuple(docName);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getDocName());
                    buffer.incrementTuple();
                }
            }  
            public final void flushScore(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getScoreEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeDouble(buffer.getScore());
                    output.writeInt(count);
                    buffer.incrementScore();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushScore(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TDoubleArrayList scores = new TDoubleArrayList();
            TIntArrayList scoreTupleIdx = new TIntArrayList();
            int scoreReadIdx = 0;
                            
            String[] docNames;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                docNames = new String[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processScore(double score) {
                scores.add(score);
                scoreTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String docName) {
                assert scores.size() > 0;
                docNames[writeTupleIndex] = docName;
                writeTupleIndex++;
            }
            public void resetData() {
                scores.clear();
                scoreTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                scoreReadIdx = 0;
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
            public void incrementScore() {
                scoreReadIdx++;  
            }                                                                                              

            public void autoIncrementScore() {
                while (readTupleIndex >= getScoreEndIndex() && readTupleIndex < writeTupleIndex)
                    scoreReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getScoreEndIndex() {
                if ((scoreReadIdx+1) >= scoreTupleIdx.size())
                    return writeTupleIndex;
                return scoreTupleIdx.get(scoreReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public double getScore() {
                assert readTupleIndex < writeTupleIndex;
                assert scoreReadIdx < scores.size();
                
                return scores.get(scoreReadIdx);
            }
            public String getDocName() {
                assert readTupleIndex < writeTupleIndex;
                return docNames[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getDocName());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexScore(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processScore(getScore());
                    assert getScoreEndIndex() <= endIndex;
                    copyTuples(getScoreEndIndex(), output);
                    incrementScore();
                }
            }  
            public void copyUntilScore(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getScore(), other.getScore());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processScore(getScore());
                                      
                        copyTuples(getScoreEndIndex(), output);
                    } else {
                        output.processScore(getScore());
                        copyTuples(getScoreEndIndex(), output);
                    }
                    incrementScore();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilScore(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<PageRankScore>, ShreddedSource {
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
                } else if (processor instanceof PageRankScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankScore> getOutputClass() {
                return PageRankScore.class;
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

            public PageRankScore read() throws IOException {
                if (uninitialized)
                    initialize();

                PageRankScore result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<PageRankScore>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            PageRankScore last = new PageRankScore();         
            long updateScoreCount = -1;
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
                    result = + CmpUtil.compare(buffer.getScore(), otherBuffer.getScore());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final PageRankScore read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                PageRankScore result = new PageRankScore();
                
                result.score = buffer.getScore();
                result.docName = buffer.getDocName();
                
                buffer.incrementTuple();
                buffer.autoIncrementScore();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateScoreCount - tupleCount > 0) {
                            buffer.scores.add(last.score);
                            buffer.scoreTupleIdx.add((int) (updateScoreCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateScore();
                        buffer.processTuple(input.readString());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateScore() throws IOException {
                if (updateScoreCount > tupleCount)
                    return;
                     
                last.score = input.readDouble();
                updateScoreCount = tupleCount + input.readInt();
                                      
                buffer.processScore(last.score);
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
                } else if (processor instanceof PageRankScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankScore> getOutputClass() {
                return PageRankScore.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            PageRankScore last = new PageRankScore();
            boolean scoreProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processScore(double score) throws IOException {  
                if (scoreProcess || CmpUtil.compare(score, last.score) != 0) {
                    last.score = score;
                    processor.processScore(score);
                    scoreProcess = false;
                }
            }  
            
            public void resetScore() {
                 scoreProcess = true;
            }                                                
                               
            public void processTuple(String docName) throws IOException {
                processor.processTuple(docName);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            PageRankScore last = new PageRankScore();
            public org.lemurproject.galago.tupleflow.Processor<PageRankScore> processor;                               
            
            public TupleUnshredder(PageRankScore.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<PageRankScore> processor) {
                this.processor = processor;
            }
            
            public PageRankScore clone(PageRankScore object) {
                PageRankScore result = new PageRankScore();
                if (object == null) return result;
                result.docName = object.docName; 
                result.score = object.score; 
                return result;
            }                 
            
            public void processScore(double score) throws IOException {
                last.score = score;
            }   
                
            
            public void processTuple(String docName) throws IOException {
                last.docName = docName;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            PageRankScore last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public PageRankScore clone(PageRankScore object) {
                PageRankScore result = new PageRankScore();
                if (object == null) return result;
                result.docName = object.docName; 
                result.score = object.score; 
                return result;
            }                 
            
            public void process(PageRankScore object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.score, object.score) != 0 || processAll) { processor.processScore(object.score); processAll = true; }
                processor.processTuple(object.docName);                                         
                last = object;
            }
                          
            public Class<PageRankScore> getInputClass() {
                return PageRankScore.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class DescScoreOrder implements Order<PageRankScore> {
        public int hash(PageRankScore object) {
            int h = 0;
            h += CmpUtil.hash(object.score);
            return h;
        } 
        public Comparator<PageRankScore> greaterThan() {
            return new Comparator<PageRankScore>() {
                public int compare(PageRankScore one, PageRankScore two) {
                    int result = 0;
                    do {
                        result = - CmpUtil.compare(one.score, two.score);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<PageRankScore> lessThan() {
            return new Comparator<PageRankScore>() {
                public int compare(PageRankScore one, PageRankScore two) {
                    int result = 0;
                    do {
                        result = - CmpUtil.compare(one.score, two.score);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<PageRankScore> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<PageRankScore> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<PageRankScore> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< PageRankScore > {
            PageRankScore last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(PageRankScore object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.score, last.score)) { processAll = true; shreddedWriter.processScore(object.score); }
               shreddedWriter.processTuple(object.docName);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<PageRankScore> getInputClass() {
                return PageRankScore.class;
            }
        } 
        public ReaderSource<PageRankScore> orderedCombiner(Collection<TypeReader<PageRankScore>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<PageRankScore> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public PageRankScore clone(PageRankScore object) {
            PageRankScore result = new PageRankScore();
            if (object == null) return result;
            result.docName = object.docName; 
            result.score = object.score; 
            return result;
        }                 
        public Class<PageRankScore> getOrderedClass() {
            return PageRankScore.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"-score"};
        }

        public static String[] getSpec() {
            return new String[] {"-score"};
        }
        public static String getSpecString() {
            return "-score";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processScore(double score) throws IOException;
            public void processTuple(String docName) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            double lastScore;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processScore(double score) {
                lastScore = score;
                buffer.processScore(score);
            }
            public final void processTuple(String docName) throws IOException {
                if (lastFlush) {
                    if(buffer.scores.size() == 0) buffer.processScore(lastScore);
                    lastFlush = false;
                }
                buffer.processTuple(docName);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getDocName());
                    buffer.incrementTuple();
                }
            }  
            public final void flushScore(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getScoreEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeDouble(buffer.getScore());
                    output.writeInt(count);
                    buffer.incrementScore();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushScore(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TDoubleArrayList scores = new TDoubleArrayList();
            TIntArrayList scoreTupleIdx = new TIntArrayList();
            int scoreReadIdx = 0;
                            
            String[] docNames;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                docNames = new String[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processScore(double score) {
                scores.add(score);
                scoreTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String docName) {
                assert scores.size() > 0;
                docNames[writeTupleIndex] = docName;
                writeTupleIndex++;
            }
            public void resetData() {
                scores.clear();
                scoreTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                scoreReadIdx = 0;
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
            public void incrementScore() {
                scoreReadIdx++;  
            }                                                                                              

            public void autoIncrementScore() {
                while (readTupleIndex >= getScoreEndIndex() && readTupleIndex < writeTupleIndex)
                    scoreReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getScoreEndIndex() {
                if ((scoreReadIdx+1) >= scoreTupleIdx.size())
                    return writeTupleIndex;
                return scoreTupleIdx.get(scoreReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public double getScore() {
                assert readTupleIndex < writeTupleIndex;
                assert scoreReadIdx < scores.size();
                
                return scores.get(scoreReadIdx);
            }
            public String getDocName() {
                assert readTupleIndex < writeTupleIndex;
                return docNames[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getDocName());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexScore(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processScore(getScore());
                    assert getScoreEndIndex() <= endIndex;
                    copyTuples(getScoreEndIndex(), output);
                    incrementScore();
                }
            }  
            public void copyUntilScore(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = - CmpUtil.compare(getScore(), other.getScore());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processScore(getScore());
                                      
                        copyTuples(getScoreEndIndex(), output);
                    } else {
                        output.processScore(getScore());
                        copyTuples(getScoreEndIndex(), output);
                    }
                    incrementScore();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilScore(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<PageRankScore>, ShreddedSource {
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
                } else if (processor instanceof PageRankScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankScore> getOutputClass() {
                return PageRankScore.class;
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

            public PageRankScore read() throws IOException {
                if (uninitialized)
                    initialize();

                PageRankScore result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<PageRankScore>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            PageRankScore last = new PageRankScore();         
            long updateScoreCount = -1;
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
                    result = - CmpUtil.compare(buffer.getScore(), otherBuffer.getScore());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final PageRankScore read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                PageRankScore result = new PageRankScore();
                
                result.score = buffer.getScore();
                result.docName = buffer.getDocName();
                
                buffer.incrementTuple();
                buffer.autoIncrementScore();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateScoreCount - tupleCount > 0) {
                            buffer.scores.add(last.score);
                            buffer.scoreTupleIdx.add((int) (updateScoreCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateScore();
                        buffer.processTuple(input.readString());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateScore() throws IOException {
                if (updateScoreCount > tupleCount)
                    return;
                     
                last.score = input.readDouble();
                updateScoreCount = tupleCount + input.readInt();
                                      
                buffer.processScore(last.score);
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
                } else if (processor instanceof PageRankScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankScore> getOutputClass() {
                return PageRankScore.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            PageRankScore last = new PageRankScore();
            boolean scoreProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processScore(double score) throws IOException {  
                if (scoreProcess || CmpUtil.compare(score, last.score) != 0) {
                    last.score = score;
                    processor.processScore(score);
                    scoreProcess = false;
                }
            }  
            
            public void resetScore() {
                 scoreProcess = true;
            }                                                
                               
            public void processTuple(String docName) throws IOException {
                processor.processTuple(docName);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            PageRankScore last = new PageRankScore();
            public org.lemurproject.galago.tupleflow.Processor<PageRankScore> processor;                               
            
            public TupleUnshredder(PageRankScore.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<PageRankScore> processor) {
                this.processor = processor;
            }
            
            public PageRankScore clone(PageRankScore object) {
                PageRankScore result = new PageRankScore();
                if (object == null) return result;
                result.docName = object.docName; 
                result.score = object.score; 
                return result;
            }                 
            
            public void processScore(double score) throws IOException {
                last.score = score;
            }   
                
            
            public void processTuple(String docName) throws IOException {
                last.docName = docName;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            PageRankScore last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public PageRankScore clone(PageRankScore object) {
                PageRankScore result = new PageRankScore();
                if (object == null) return result;
                result.docName = object.docName; 
                result.score = object.score; 
                return result;
            }                 
            
            public void process(PageRankScore object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.score, object.score) != 0 || processAll) { processor.processScore(object.score); processAll = true; }
                processor.processTuple(object.docName);                                         
                last = object;
            }
                          
            public Class<PageRankScore> getInputClass() {
                return PageRankScore.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    