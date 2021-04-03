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
 * Tupleflow-Typebuilder automatically-generated class: ExtractedLink.
 */
@SuppressWarnings({"unused","unchecked"})
public final class ExtractedLink implements Type<ExtractedLink> {
    public String srcUrl;
    public String srcName;
    public String destUrl;
    public String destName;
    public String anchorText;
    public boolean noFollow; 
    
    /** default constructor makes most fields null */
    public ExtractedLink() {}
    /** additional constructor takes all fields explicitly */
    public ExtractedLink(String srcUrl, String srcName, String destUrl, String destName, String anchorText, boolean noFollow) {
        this.srcUrl = srcUrl;
        this.srcName = srcName;
        this.destUrl = destUrl;
        this.destName = destName;
        this.anchorText = anchorText;
        this.noFollow = noFollow;
    }  
    
    public String toString() {
            return String.format("%s,%s,%s,%s,%s,%b",
                                   srcUrl, srcName, destUrl, destName, anchorText, noFollow);
    } 

    public Order<ExtractedLink> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+destUrl" })) {
            return new DestUrlOrder();
        }
        if (Arrays.equals(spec, new String[] { "+srcUrl" })) {
            return new SrcUrlOrder();
        }
        if (Arrays.equals(spec, new String[] { "+srcName" })) {
            return new SrcNameOrder();
        }
        if (Arrays.equals(spec, new String[] { "+destName" })) {
            return new DestNameOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<ExtractedLink> {
        public void process(ExtractedLink object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class DestUrlOrder implements Order<ExtractedLink> {
        public int hash(ExtractedLink object) {
            int h = 0;
            h += CmpUtil.hash(object.destUrl);
            return h;
        } 
        public Comparator<ExtractedLink> greaterThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destUrl, two.destUrl);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLink> lessThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destUrl, two.destUrl);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLink> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLink > {
            ExtractedLink last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLink object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.destUrl, last.destUrl)) { processAll = true; shreddedWriter.processDestUrl(object.destUrl); }
               shreddedWriter.processTuple(object.srcUrl, object.srcName, object.destName, object.anchorText, object.noFollow);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }
        } 
        public ReaderSource<ExtractedLink> orderedCombiner(Collection<TypeReader<ExtractedLink>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLink> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLink clone(ExtractedLink object) {
            ExtractedLink result = new ExtractedLink();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            return result;
        }                 
        public Class<ExtractedLink> getOrderedClass() {
            return ExtractedLink.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+destUrl"};
        }

        public static String[] getSpec() {
            return new String[] {"+destUrl"};
        }
        public static String getSpecString() {
            return "+destUrl";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processDestUrl(String destUrl) throws IOException;
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastDestUrl;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processDestUrl(String destUrl) {
                lastDestUrl = destUrl;
                buffer.processDestUrl(destUrl);
            }
            public final void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow) throws IOException {
                if (lastFlush) {
                    if(buffer.destUrls.size() == 0) buffer.processDestUrl(lastDestUrl);
                    lastFlush = false;
                }
                buffer.processTuple(srcUrl, srcName, destName, anchorText, noFollow);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getSrcUrl());
                    output.writeString(buffer.getSrcName());
                    output.writeString(buffer.getDestName());
                    output.writeString(buffer.getAnchorText());
                    output.writeBoolean(buffer.getNoFollow());
                    buffer.incrementTuple();
                }
            }  
            public final void flushDestUrl(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getDestUrlEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getDestUrl());
                    output.writeInt(count);
                    buffer.incrementDestUrl();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushDestUrl(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> destUrls = new ArrayList<String>();
            TIntArrayList destUrlTupleIdx = new TIntArrayList();
            int destUrlReadIdx = 0;
                            
            String[] srcUrls;
            String[] srcNames;
            String[] destNames;
            String[] anchorTexts;
            boolean[] noFollows;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                srcUrls = new String[batchSize];
                srcNames = new String[batchSize];
                destNames = new String[batchSize];
                anchorTexts = new String[batchSize];
                noFollows = new boolean[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDestUrl(String destUrl) {
                destUrls.add(destUrl);
                destUrlTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow) {
                assert destUrls.size() > 0;
                srcUrls[writeTupleIndex] = srcUrl;
                srcNames[writeTupleIndex] = srcName;
                destNames[writeTupleIndex] = destName;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                writeTupleIndex++;
            }
            public void resetData() {
                destUrls.clear();
                destUrlTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                destUrlReadIdx = 0;
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
            public void incrementDestUrl() {
                destUrlReadIdx++;  
            }                                                                                              

            public void autoIncrementDestUrl() {
                while (readTupleIndex >= getDestUrlEndIndex() && readTupleIndex < writeTupleIndex)
                    destUrlReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getDestUrlEndIndex() {
                if ((destUrlReadIdx+1) >= destUrlTupleIdx.size())
                    return writeTupleIndex;
                return destUrlTupleIdx.get(destUrlReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getDestUrl() {
                assert readTupleIndex < writeTupleIndex;
                assert destUrlReadIdx < destUrls.size();
                
                return destUrls.get(destUrlReadIdx);
            }
            public String getSrcUrl() {
                assert readTupleIndex < writeTupleIndex;
                return srcUrls[readTupleIndex];
            }                                         
            public String getSrcName() {
                assert readTupleIndex < writeTupleIndex;
                return srcNames[readTupleIndex];
            }                                         
            public String getDestName() {
                assert readTupleIndex < writeTupleIndex;
                return destNames[readTupleIndex];
            }                                         
            public String getAnchorText() {
                assert readTupleIndex < writeTupleIndex;
                return anchorTexts[readTupleIndex];
            }                                         
            public boolean getNoFollow() {
                assert readTupleIndex < writeTupleIndex;
                return noFollows[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcUrl(), getSrcName(), getDestName(), getAnchorText(), getNoFollow());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexDestUrl(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processDestUrl(getDestUrl());
                    assert getDestUrlEndIndex() <= endIndex;
                    copyTuples(getDestUrlEndIndex(), output);
                    incrementDestUrl();
                }
            }  
            public void copyUntilDestUrl(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getDestUrl(), other.getDestUrl());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processDestUrl(getDestUrl());
                                      
                        copyTuples(getDestUrlEndIndex(), output);
                    } else {
                        output.processDestUrl(getDestUrl());
                        copyTuples(getDestUrlEndIndex(), output);
                    }
                    incrementDestUrl();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilDestUrl(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLink>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
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

            public ExtractedLink read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLink result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLink>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLink last = new ExtractedLink();         
            long updateDestUrlCount = -1;
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
                    result = + CmpUtil.compare(buffer.getDestUrl(), otherBuffer.getDestUrl());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final ExtractedLink read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLink result = new ExtractedLink();
                
                result.destUrl = buffer.getDestUrl();
                result.srcUrl = buffer.getSrcUrl();
                result.srcName = buffer.getSrcName();
                result.destName = buffer.getDestName();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                
                buffer.incrementTuple();
                buffer.autoIncrementDestUrl();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateDestUrlCount - tupleCount > 0) {
                            buffer.destUrls.add(last.destUrl);
                            buffer.destUrlTupleIdx.add((int) (updateDestUrlCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateDestUrl();
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateDestUrl() throws IOException {
                if (updateDestUrlCount > tupleCount)
                    return;
                     
                last.destUrl = input.readString();
                updateDestUrlCount = tupleCount + input.readInt();
                                      
                buffer.processDestUrl(last.destUrl);
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLink last = new ExtractedLink();
            boolean destUrlProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processDestUrl(String destUrl) throws IOException {  
                if (destUrlProcess || CmpUtil.compare(destUrl, last.destUrl) != 0) {
                    last.destUrl = destUrl;
                    processor.processDestUrl(destUrl);
                    destUrlProcess = false;
                }
            }  
            
            public void resetDestUrl() {
                 destUrlProcess = true;
            }                                                
                               
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow) throws IOException {
                processor.processTuple(srcUrl, srcName, destName, anchorText, noFollow);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLink last = new ExtractedLink();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor;                               
            
            public TupleUnshredder(ExtractedLink.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor) {
                this.processor = processor;
            }
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void processDestUrl(String destUrl) throws IOException {
                last.destUrl = destUrl;
            }   
                
            
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow) throws IOException {
                last.srcUrl = srcUrl;
                last.srcName = srcName;
                last.destName = destName;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLink last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void process(ExtractedLink object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.destUrl, object.destUrl) != 0 || processAll) { processor.processDestUrl(object.destUrl); processAll = true; }
                processor.processTuple(object.srcUrl, object.srcName, object.destName, object.anchorText, object.noFollow);                                         
                last = object;
            }
                          
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class SrcUrlOrder implements Order<ExtractedLink> {
        public int hash(ExtractedLink object) {
            int h = 0;
            h += CmpUtil.hash(object.srcUrl);
            return h;
        } 
        public Comparator<ExtractedLink> greaterThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcUrl, two.srcUrl);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLink> lessThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcUrl, two.srcUrl);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLink> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLink > {
            ExtractedLink last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLink object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.srcUrl, last.srcUrl)) { processAll = true; shreddedWriter.processSrcUrl(object.srcUrl); }
               shreddedWriter.processTuple(object.srcName, object.destUrl, object.destName, object.anchorText, object.noFollow);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }
        } 
        public ReaderSource<ExtractedLink> orderedCombiner(Collection<TypeReader<ExtractedLink>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLink> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLink clone(ExtractedLink object) {
            ExtractedLink result = new ExtractedLink();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            return result;
        }                 
        public Class<ExtractedLink> getOrderedClass() {
            return ExtractedLink.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+srcUrl"};
        }

        public static String[] getSpec() {
            return new String[] {"+srcUrl"};
        }
        public static String getSpecString() {
            return "+srcUrl";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processSrcUrl(String srcUrl) throws IOException;
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastSrcUrl;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processSrcUrl(String srcUrl) {
                lastSrcUrl = srcUrl;
                buffer.processSrcUrl(srcUrl);
            }
            public final void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                if (lastFlush) {
                    if(buffer.srcUrls.size() == 0) buffer.processSrcUrl(lastSrcUrl);
                    lastFlush = false;
                }
                buffer.processTuple(srcName, destUrl, destName, anchorText, noFollow);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getSrcName());
                    output.writeString(buffer.getDestUrl());
                    output.writeString(buffer.getDestName());
                    output.writeString(buffer.getAnchorText());
                    output.writeBoolean(buffer.getNoFollow());
                    buffer.incrementTuple();
                }
            }  
            public final void flushSrcUrl(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getSrcUrlEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getSrcUrl());
                    output.writeInt(count);
                    buffer.incrementSrcUrl();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushSrcUrl(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> srcUrls = new ArrayList<String>();
            TIntArrayList srcUrlTupleIdx = new TIntArrayList();
            int srcUrlReadIdx = 0;
                            
            String[] srcNames;
            String[] destUrls;
            String[] destNames;
            String[] anchorTexts;
            boolean[] noFollows;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                srcNames = new String[batchSize];
                destUrls = new String[batchSize];
                destNames = new String[batchSize];
                anchorTexts = new String[batchSize];
                noFollows = new boolean[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processSrcUrl(String srcUrl) {
                srcUrls.add(srcUrl);
                srcUrlTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow) {
                assert srcUrls.size() > 0;
                srcNames[writeTupleIndex] = srcName;
                destUrls[writeTupleIndex] = destUrl;
                destNames[writeTupleIndex] = destName;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                writeTupleIndex++;
            }
            public void resetData() {
                srcUrls.clear();
                srcUrlTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                srcUrlReadIdx = 0;
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
            public void incrementSrcUrl() {
                srcUrlReadIdx++;  
            }                                                                                              

            public void autoIncrementSrcUrl() {
                while (readTupleIndex >= getSrcUrlEndIndex() && readTupleIndex < writeTupleIndex)
                    srcUrlReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getSrcUrlEndIndex() {
                if ((srcUrlReadIdx+1) >= srcUrlTupleIdx.size())
                    return writeTupleIndex;
                return srcUrlTupleIdx.get(srcUrlReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getSrcUrl() {
                assert readTupleIndex < writeTupleIndex;
                assert srcUrlReadIdx < srcUrls.size();
                
                return srcUrls.get(srcUrlReadIdx);
            }
            public String getSrcName() {
                assert readTupleIndex < writeTupleIndex;
                return srcNames[readTupleIndex];
            }                                         
            public String getDestUrl() {
                assert readTupleIndex < writeTupleIndex;
                return destUrls[readTupleIndex];
            }                                         
            public String getDestName() {
                assert readTupleIndex < writeTupleIndex;
                return destNames[readTupleIndex];
            }                                         
            public String getAnchorText() {
                assert readTupleIndex < writeTupleIndex;
                return anchorTexts[readTupleIndex];
            }                                         
            public boolean getNoFollow() {
                assert readTupleIndex < writeTupleIndex;
                return noFollows[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcName(), getDestUrl(), getDestName(), getAnchorText(), getNoFollow());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexSrcUrl(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processSrcUrl(getSrcUrl());
                    assert getSrcUrlEndIndex() <= endIndex;
                    copyTuples(getSrcUrlEndIndex(), output);
                    incrementSrcUrl();
                }
            }  
            public void copyUntilSrcUrl(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getSrcUrl(), other.getSrcUrl());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processSrcUrl(getSrcUrl());
                                      
                        copyTuples(getSrcUrlEndIndex(), output);
                    } else {
                        output.processSrcUrl(getSrcUrl());
                        copyTuples(getSrcUrlEndIndex(), output);
                    }
                    incrementSrcUrl();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilSrcUrl(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLink>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
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

            public ExtractedLink read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLink result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLink>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLink last = new ExtractedLink();         
            long updateSrcUrlCount = -1;
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
                    result = + CmpUtil.compare(buffer.getSrcUrl(), otherBuffer.getSrcUrl());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final ExtractedLink read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLink result = new ExtractedLink();
                
                result.srcUrl = buffer.getSrcUrl();
                result.srcName = buffer.getSrcName();
                result.destUrl = buffer.getDestUrl();
                result.destName = buffer.getDestName();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                
                buffer.incrementTuple();
                buffer.autoIncrementSrcUrl();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateSrcUrlCount - tupleCount > 0) {
                            buffer.srcUrls.add(last.srcUrl);
                            buffer.srcUrlTupleIdx.add((int) (updateSrcUrlCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateSrcUrl();
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateSrcUrl() throws IOException {
                if (updateSrcUrlCount > tupleCount)
                    return;
                     
                last.srcUrl = input.readString();
                updateSrcUrlCount = tupleCount + input.readInt();
                                      
                buffer.processSrcUrl(last.srcUrl);
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLink last = new ExtractedLink();
            boolean srcUrlProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processSrcUrl(String srcUrl) throws IOException {  
                if (srcUrlProcess || CmpUtil.compare(srcUrl, last.srcUrl) != 0) {
                    last.srcUrl = srcUrl;
                    processor.processSrcUrl(srcUrl);
                    srcUrlProcess = false;
                }
            }  
            
            public void resetSrcUrl() {
                 srcUrlProcess = true;
            }                                                
                               
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                processor.processTuple(srcName, destUrl, destName, anchorText, noFollow);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLink last = new ExtractedLink();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor;                               
            
            public TupleUnshredder(ExtractedLink.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor) {
                this.processor = processor;
            }
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void processSrcUrl(String srcUrl) throws IOException {
                last.srcUrl = srcUrl;
            }   
                
            
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                last.srcName = srcName;
                last.destUrl = destUrl;
                last.destName = destName;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLink last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void process(ExtractedLink object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.srcUrl, object.srcUrl) != 0 || processAll) { processor.processSrcUrl(object.srcUrl); processAll = true; }
                processor.processTuple(object.srcName, object.destUrl, object.destName, object.anchorText, object.noFollow);                                         
                last = object;
            }
                          
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class SrcNameOrder implements Order<ExtractedLink> {
        public int hash(ExtractedLink object) {
            int h = 0;
            h += CmpUtil.hash(object.srcName);
            return h;
        } 
        public Comparator<ExtractedLink> greaterThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcName, two.srcName);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLink> lessThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcName, two.srcName);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLink> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLink > {
            ExtractedLink last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLink object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.srcName, last.srcName)) { processAll = true; shreddedWriter.processSrcName(object.srcName); }
               shreddedWriter.processTuple(object.srcUrl, object.destUrl, object.destName, object.anchorText, object.noFollow);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }
        } 
        public ReaderSource<ExtractedLink> orderedCombiner(Collection<TypeReader<ExtractedLink>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLink> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLink clone(ExtractedLink object) {
            ExtractedLink result = new ExtractedLink();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            return result;
        }                 
        public Class<ExtractedLink> getOrderedClass() {
            return ExtractedLink.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+srcName"};
        }

        public static String[] getSpec() {
            return new String[] {"+srcName"};
        }
        public static String getSpecString() {
            return "+srcName";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processSrcName(String srcName) throws IOException;
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastSrcName;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processSrcName(String srcName) {
                lastSrcName = srcName;
                buffer.processSrcName(srcName);
            }
            public final void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                if (lastFlush) {
                    if(buffer.srcNames.size() == 0) buffer.processSrcName(lastSrcName);
                    lastFlush = false;
                }
                buffer.processTuple(srcUrl, destUrl, destName, anchorText, noFollow);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getSrcUrl());
                    output.writeString(buffer.getDestUrl());
                    output.writeString(buffer.getDestName());
                    output.writeString(buffer.getAnchorText());
                    output.writeBoolean(buffer.getNoFollow());
                    buffer.incrementTuple();
                }
            }  
            public final void flushSrcName(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getSrcNameEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getSrcName());
                    output.writeInt(count);
                    buffer.incrementSrcName();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushSrcName(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> srcNames = new ArrayList<String>();
            TIntArrayList srcNameTupleIdx = new TIntArrayList();
            int srcNameReadIdx = 0;
                            
            String[] srcUrls;
            String[] destUrls;
            String[] destNames;
            String[] anchorTexts;
            boolean[] noFollows;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                srcUrls = new String[batchSize];
                destUrls = new String[batchSize];
                destNames = new String[batchSize];
                anchorTexts = new String[batchSize];
                noFollows = new boolean[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processSrcName(String srcName) {
                srcNames.add(srcName);
                srcNameTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow) {
                assert srcNames.size() > 0;
                srcUrls[writeTupleIndex] = srcUrl;
                destUrls[writeTupleIndex] = destUrl;
                destNames[writeTupleIndex] = destName;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                writeTupleIndex++;
            }
            public void resetData() {
                srcNames.clear();
                srcNameTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                srcNameReadIdx = 0;
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
            public void incrementSrcName() {
                srcNameReadIdx++;  
            }                                                                                              

            public void autoIncrementSrcName() {
                while (readTupleIndex >= getSrcNameEndIndex() && readTupleIndex < writeTupleIndex)
                    srcNameReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getSrcNameEndIndex() {
                if ((srcNameReadIdx+1) >= srcNameTupleIdx.size())
                    return writeTupleIndex;
                return srcNameTupleIdx.get(srcNameReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getSrcName() {
                assert readTupleIndex < writeTupleIndex;
                assert srcNameReadIdx < srcNames.size();
                
                return srcNames.get(srcNameReadIdx);
            }
            public String getSrcUrl() {
                assert readTupleIndex < writeTupleIndex;
                return srcUrls[readTupleIndex];
            }                                         
            public String getDestUrl() {
                assert readTupleIndex < writeTupleIndex;
                return destUrls[readTupleIndex];
            }                                         
            public String getDestName() {
                assert readTupleIndex < writeTupleIndex;
                return destNames[readTupleIndex];
            }                                         
            public String getAnchorText() {
                assert readTupleIndex < writeTupleIndex;
                return anchorTexts[readTupleIndex];
            }                                         
            public boolean getNoFollow() {
                assert readTupleIndex < writeTupleIndex;
                return noFollows[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcUrl(), getDestUrl(), getDestName(), getAnchorText(), getNoFollow());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexSrcName(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processSrcName(getSrcName());
                    assert getSrcNameEndIndex() <= endIndex;
                    copyTuples(getSrcNameEndIndex(), output);
                    incrementSrcName();
                }
            }  
            public void copyUntilSrcName(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getSrcName(), other.getSrcName());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processSrcName(getSrcName());
                                      
                        copyTuples(getSrcNameEndIndex(), output);
                    } else {
                        output.processSrcName(getSrcName());
                        copyTuples(getSrcNameEndIndex(), output);
                    }
                    incrementSrcName();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilSrcName(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLink>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
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

            public ExtractedLink read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLink result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLink>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLink last = new ExtractedLink();         
            long updateSrcNameCount = -1;
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
                    result = + CmpUtil.compare(buffer.getSrcName(), otherBuffer.getSrcName());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final ExtractedLink read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLink result = new ExtractedLink();
                
                result.srcName = buffer.getSrcName();
                result.srcUrl = buffer.getSrcUrl();
                result.destUrl = buffer.getDestUrl();
                result.destName = buffer.getDestName();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                
                buffer.incrementTuple();
                buffer.autoIncrementSrcName();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateSrcNameCount - tupleCount > 0) {
                            buffer.srcNames.add(last.srcName);
                            buffer.srcNameTupleIdx.add((int) (updateSrcNameCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateSrcName();
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateSrcName() throws IOException {
                if (updateSrcNameCount > tupleCount)
                    return;
                     
                last.srcName = input.readString();
                updateSrcNameCount = tupleCount + input.readInt();
                                      
                buffer.processSrcName(last.srcName);
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLink last = new ExtractedLink();
            boolean srcNameProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processSrcName(String srcName) throws IOException {  
                if (srcNameProcess || CmpUtil.compare(srcName, last.srcName) != 0) {
                    last.srcName = srcName;
                    processor.processSrcName(srcName);
                    srcNameProcess = false;
                }
            }  
            
            public void resetSrcName() {
                 srcNameProcess = true;
            }                                                
                               
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                processor.processTuple(srcUrl, destUrl, destName, anchorText, noFollow);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLink last = new ExtractedLink();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor;                               
            
            public TupleUnshredder(ExtractedLink.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor) {
                this.processor = processor;
            }
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void processSrcName(String srcName) throws IOException {
                last.srcName = srcName;
            }   
                
            
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                last.srcUrl = srcUrl;
                last.destUrl = destUrl;
                last.destName = destName;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLink last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void process(ExtractedLink object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.srcName, object.srcName) != 0 || processAll) { processor.processSrcName(object.srcName); processAll = true; }
                processor.processTuple(object.srcUrl, object.destUrl, object.destName, object.anchorText, object.noFollow);                                         
                last = object;
            }
                          
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class DestNameOrder implements Order<ExtractedLink> {
        public int hash(ExtractedLink object) {
            int h = 0;
            h += CmpUtil.hash(object.destName);
            return h;
        } 
        public Comparator<ExtractedLink> greaterThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destName, two.destName);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLink> lessThan() {
            return new Comparator<ExtractedLink>() {
                public int compare(ExtractedLink one, ExtractedLink two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destName, two.destName);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLink> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLink> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLink > {
            ExtractedLink last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLink object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.destName, last.destName)) { processAll = true; shreddedWriter.processDestName(object.destName); }
               shreddedWriter.processTuple(object.srcUrl, object.srcName, object.destUrl, object.anchorText, object.noFollow);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }
        } 
        public ReaderSource<ExtractedLink> orderedCombiner(Collection<TypeReader<ExtractedLink>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLink> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLink clone(ExtractedLink object) {
            ExtractedLink result = new ExtractedLink();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            return result;
        }                 
        public Class<ExtractedLink> getOrderedClass() {
            return ExtractedLink.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+destName"};
        }

        public static String[] getSpec() {
            return new String[] {"+destName"};
        }
        public static String getSpecString() {
            return "+destName";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processDestName(String destName) throws IOException;
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastDestName;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processDestName(String destName) {
                lastDestName = destName;
                buffer.processDestName(destName);
            }
            public final void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow) throws IOException {
                if (lastFlush) {
                    if(buffer.destNames.size() == 0) buffer.processDestName(lastDestName);
                    lastFlush = false;
                }
                buffer.processTuple(srcUrl, srcName, destUrl, anchorText, noFollow);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getSrcUrl());
                    output.writeString(buffer.getSrcName());
                    output.writeString(buffer.getDestUrl());
                    output.writeString(buffer.getAnchorText());
                    output.writeBoolean(buffer.getNoFollow());
                    buffer.incrementTuple();
                }
            }  
            public final void flushDestName(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getDestNameEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getDestName());
                    output.writeInt(count);
                    buffer.incrementDestName();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushDestName(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> destNames = new ArrayList<String>();
            TIntArrayList destNameTupleIdx = new TIntArrayList();
            int destNameReadIdx = 0;
                            
            String[] srcUrls;
            String[] srcNames;
            String[] destUrls;
            String[] anchorTexts;
            boolean[] noFollows;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                srcUrls = new String[batchSize];
                srcNames = new String[batchSize];
                destUrls = new String[batchSize];
                anchorTexts = new String[batchSize];
                noFollows = new boolean[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDestName(String destName) {
                destNames.add(destName);
                destNameTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow) {
                assert destNames.size() > 0;
                srcUrls[writeTupleIndex] = srcUrl;
                srcNames[writeTupleIndex] = srcName;
                destUrls[writeTupleIndex] = destUrl;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                writeTupleIndex++;
            }
            public void resetData() {
                destNames.clear();
                destNameTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                destNameReadIdx = 0;
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
            public void incrementDestName() {
                destNameReadIdx++;  
            }                                                                                              

            public void autoIncrementDestName() {
                while (readTupleIndex >= getDestNameEndIndex() && readTupleIndex < writeTupleIndex)
                    destNameReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getDestNameEndIndex() {
                if ((destNameReadIdx+1) >= destNameTupleIdx.size())
                    return writeTupleIndex;
                return destNameTupleIdx.get(destNameReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getDestName() {
                assert readTupleIndex < writeTupleIndex;
                assert destNameReadIdx < destNames.size();
                
                return destNames.get(destNameReadIdx);
            }
            public String getSrcUrl() {
                assert readTupleIndex < writeTupleIndex;
                return srcUrls[readTupleIndex];
            }                                         
            public String getSrcName() {
                assert readTupleIndex < writeTupleIndex;
                return srcNames[readTupleIndex];
            }                                         
            public String getDestUrl() {
                assert readTupleIndex < writeTupleIndex;
                return destUrls[readTupleIndex];
            }                                         
            public String getAnchorText() {
                assert readTupleIndex < writeTupleIndex;
                return anchorTexts[readTupleIndex];
            }                                         
            public boolean getNoFollow() {
                assert readTupleIndex < writeTupleIndex;
                return noFollows[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcUrl(), getSrcName(), getDestUrl(), getAnchorText(), getNoFollow());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexDestName(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processDestName(getDestName());
                    assert getDestNameEndIndex() <= endIndex;
                    copyTuples(getDestNameEndIndex(), output);
                    incrementDestName();
                }
            }  
            public void copyUntilDestName(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getDestName(), other.getDestName());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processDestName(getDestName());
                                      
                        copyTuples(getDestNameEndIndex(), output);
                    } else {
                        output.processDestName(getDestName());
                        copyTuples(getDestNameEndIndex(), output);
                    }
                    incrementDestName();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilDestName(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLink>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
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

            public ExtractedLink read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLink result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLink>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLink last = new ExtractedLink();         
            long updateDestNameCount = -1;
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
                    result = + CmpUtil.compare(buffer.getDestName(), otherBuffer.getDestName());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final ExtractedLink read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLink result = new ExtractedLink();
                
                result.destName = buffer.getDestName();
                result.srcUrl = buffer.getSrcUrl();
                result.srcName = buffer.getSrcName();
                result.destUrl = buffer.getDestUrl();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                
                buffer.incrementTuple();
                buffer.autoIncrementDestName();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateDestNameCount - tupleCount > 0) {
                            buffer.destNames.add(last.destName);
                            buffer.destNameTupleIdx.add((int) (updateDestNameCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateDestName();
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateDestName() throws IOException {
                if (updateDestNameCount > tupleCount)
                    return;
                     
                last.destName = input.readString();
                updateDestNameCount = tupleCount + input.readInt();
                                      
                buffer.processDestName(last.destName);
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
                } else if (processor instanceof ExtractedLink.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLink.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLink>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLink> getOutputClass() {
                return ExtractedLink.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLink last = new ExtractedLink();
            boolean destNameProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processDestName(String destName) throws IOException {  
                if (destNameProcess || CmpUtil.compare(destName, last.destName) != 0) {
                    last.destName = destName;
                    processor.processDestName(destName);
                    destNameProcess = false;
                }
            }  
            
            public void resetDestName() {
                 destNameProcess = true;
            }                                                
                               
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow) throws IOException {
                processor.processTuple(srcUrl, srcName, destUrl, anchorText, noFollow);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLink last = new ExtractedLink();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor;                               
            
            public TupleUnshredder(ExtractedLink.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLink> processor) {
                this.processor = processor;
            }
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void processDestName(String destName) throws IOException {
                last.destName = destName;
            }   
                
            
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow) throws IOException {
                last.srcUrl = srcUrl;
                last.srcName = srcName;
                last.destUrl = destUrl;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLink last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLink clone(ExtractedLink object) {
                ExtractedLink result = new ExtractedLink();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                return result;
            }                 
            
            public void process(ExtractedLink object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.destName, object.destName) != 0 || processAll) { processor.processDestName(object.destName); processAll = true; }
                processor.processTuple(object.srcUrl, object.srcName, object.destUrl, object.anchorText, object.noFollow);                                         
                last = object;
            }
                          
            public Class<ExtractedLink> getInputClass() {
                return ExtractedLink.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    