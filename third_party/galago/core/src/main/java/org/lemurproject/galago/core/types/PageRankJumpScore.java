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
 * Tupleflow-Typebuilder automatically-generated class: PageRankJumpScore.
 */
@SuppressWarnings({"unused","unchecked"})
public final class PageRankJumpScore implements Type<PageRankJumpScore> {
    public double score; 
    
    /** default constructor makes most fields null */
    public PageRankJumpScore() {}
    /** additional constructor takes all fields explicitly */
    public PageRankJumpScore(double score) {
        this.score = score;
    }  
    
    public String toString() {
            return String.format("%g",
                                   score);
    } 

    public Order<PageRankJumpScore> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+score" })) {
            return new ScoreOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<PageRankJumpScore> {
        public void process(PageRankJumpScore object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class ScoreOrder implements Order<PageRankJumpScore> {
        public int hash(PageRankJumpScore object) {
            int h = 0;
            h += CmpUtil.hash(object.score);
            return h;
        } 
        public Comparator<PageRankJumpScore> greaterThan() {
            return new Comparator<PageRankJumpScore>() {
                public int compare(PageRankJumpScore one, PageRankJumpScore two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.score, two.score);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<PageRankJumpScore> lessThan() {
            return new Comparator<PageRankJumpScore>() {
                public int compare(PageRankJumpScore one, PageRankJumpScore two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.score, two.score);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<PageRankJumpScore> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<PageRankJumpScore> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<PageRankJumpScore> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< PageRankJumpScore > {
            PageRankJumpScore last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(PageRankJumpScore object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.score, last.score)) { processAll = true; shreddedWriter.processScore(object.score); }
               shreddedWriter.processTuple();
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<PageRankJumpScore> getInputClass() {
                return PageRankJumpScore.class;
            }
        } 
        public ReaderSource<PageRankJumpScore> orderedCombiner(Collection<TypeReader<PageRankJumpScore>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<PageRankJumpScore> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public PageRankJumpScore clone(PageRankJumpScore object) {
            PageRankJumpScore result = new PageRankJumpScore();
            if (object == null) return result;
            result.score = object.score; 
            return result;
        }                 
        public Class<PageRankJumpScore> getOrderedClass() {
            return PageRankJumpScore.class;
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
            public void processTuple() throws IOException;
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
            public final void processTuple() throws IOException {
                if (lastFlush) {
                    if(buffer.scores.size() == 0) buffer.processScore(lastScore);
                    lastFlush = false;
                }
                buffer.processTuple();
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
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
                            
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processScore(double score) {
                scores.add(score);
                scoreTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple() {
                assert scores.size() > 0;
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

            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple();
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
        public static final class ShreddedCombiner implements ReaderSource<PageRankJumpScore>, ShreddedSource {
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
                } else if (processor instanceof PageRankJumpScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankJumpScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankJumpScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankJumpScore> getOutputClass() {
                return PageRankJumpScore.class;
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

            public PageRankJumpScore read() throws IOException {
                if (uninitialized)
                    initialize();

                PageRankJumpScore result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<PageRankJumpScore>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            PageRankJumpScore last = new PageRankJumpScore();         
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
            
            public final PageRankJumpScore read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                PageRankJumpScore result = new PageRankJumpScore();
                
                result.score = buffer.getScore();
                
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
                        buffer.processTuple();
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
                } else if (processor instanceof PageRankJumpScore.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((PageRankJumpScore.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<PageRankJumpScore>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<PageRankJumpScore> getOutputClass() {
                return PageRankJumpScore.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            PageRankJumpScore last = new PageRankJumpScore();
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
                               
            public void processTuple() throws IOException {
                processor.processTuple();
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            PageRankJumpScore last = new PageRankJumpScore();
            public org.lemurproject.galago.tupleflow.Processor<PageRankJumpScore> processor;                               
            
            public TupleUnshredder(PageRankJumpScore.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<PageRankJumpScore> processor) {
                this.processor = processor;
            }
            
            public PageRankJumpScore clone(PageRankJumpScore object) {
                PageRankJumpScore result = new PageRankJumpScore();
                if (object == null) return result;
                result.score = object.score; 
                return result;
            }                 
            
            public void processScore(double score) throws IOException {
                last.score = score;
            }   
                
            
            public void processTuple() throws IOException {
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            PageRankJumpScore last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public PageRankJumpScore clone(PageRankJumpScore object) {
                PageRankJumpScore result = new PageRankJumpScore();
                if (object == null) return result;
                result.score = object.score; 
                return result;
            }                 
            
            public void process(PageRankJumpScore object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.score, object.score) != 0 || processAll) { processor.processScore(object.score); processAll = true; }
                processor.processTuple();                                         
                last = object;
            }
                          
            public Class<PageRankJumpScore> getInputClass() {
                return PageRankJumpScore.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    