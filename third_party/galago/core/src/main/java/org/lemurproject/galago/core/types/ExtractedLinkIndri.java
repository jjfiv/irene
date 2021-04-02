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
 * Tupleflow-Typebuilder automatically-generated class: ExtractedLinkIndri.
 */
@SuppressWarnings({"unused","unchecked"})
public final class ExtractedLinkIndri implements Type<ExtractedLinkIndri> {
    public String srcUrl;
    public String srcName;
    public String destUrl;
    public String destName;
    public String anchorText;
    public boolean noFollow;
    public String filePath;
    public long fileLocation; 
    
    /** default constructor makes most fields null */
    public ExtractedLinkIndri() {}
    /** additional constructor takes all fields explicitly */
    public ExtractedLinkIndri(String srcUrl, String srcName, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) {
        this.srcUrl = srcUrl;
        this.srcName = srcName;
        this.destUrl = destUrl;
        this.destName = destName;
        this.anchorText = anchorText;
        this.noFollow = noFollow;
        this.filePath = filePath;
        this.fileLocation = fileLocation;
    }  
    
    public String toString() {
            return String.format("%s,%s,%s,%s,%s,%b,%s,%d",
                                   srcUrl, srcName, destUrl, destName, anchorText, noFollow, filePath, fileLocation);
    } 

    public Order<ExtractedLinkIndri> getOrder(String... spec) {
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
        if (Arrays.equals(spec, new String[] { "+filePath", "+fileLocation" })) {
            return new FilePathFileLocationOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> {
        public void process(ExtractedLinkIndri object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class DestUrlOrder implements Order<ExtractedLinkIndri> {
        public int hash(ExtractedLinkIndri object) {
            int h = 0;
            h += CmpUtil.hash(object.destUrl);
            return h;
        } 
        public Comparator<ExtractedLinkIndri> greaterThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destUrl, two.destUrl);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLinkIndri> lessThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destUrl, two.destUrl);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLinkIndri> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLinkIndri > {
            ExtractedLinkIndri last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLinkIndri object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.destUrl, last.destUrl)) { processAll = true; shreddedWriter.processDestUrl(object.destUrl); }
               shreddedWriter.processTuple(object.srcUrl, object.srcName, object.destName, object.anchorText, object.noFollow, object.filePath, object.fileLocation);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }
        } 
        public ReaderSource<ExtractedLinkIndri> orderedCombiner(Collection<TypeReader<ExtractedLinkIndri>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLinkIndri> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
            ExtractedLinkIndri result = new ExtractedLinkIndri();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            result.filePath = object.filePath; 
            result.fileLocation = object.fileLocation; 
            return result;
        }                 
        public Class<ExtractedLinkIndri> getOrderedClass() {
            return ExtractedLinkIndri.class;
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
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException;
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
            public final void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                if (lastFlush) {
                    if(buffer.destUrls.size() == 0) buffer.processDestUrl(lastDestUrl);
                    lastFlush = false;
                }
                buffer.processTuple(srcUrl, srcName, destName, anchorText, noFollow, filePath, fileLocation);
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
                    output.writeString(buffer.getFilePath());
                    output.writeLong(buffer.getFileLocation());
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
            String[] filePaths;
            long[] fileLocations;
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
                filePaths = new String[batchSize];
                fileLocations = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDestUrl(String destUrl) {
                destUrls.add(destUrl);
                destUrlTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) {
                assert destUrls.size() > 0;
                srcUrls[writeTupleIndex] = srcUrl;
                srcNames[writeTupleIndex] = srcName;
                destNames[writeTupleIndex] = destName;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                filePaths[writeTupleIndex] = filePath;
                fileLocations[writeTupleIndex] = fileLocation;
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
            public String getFilePath() {
                assert readTupleIndex < writeTupleIndex;
                return filePaths[readTupleIndex];
            }                                         
            public long getFileLocation() {
                assert readTupleIndex < writeTupleIndex;
                return fileLocations[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcUrl(), getSrcName(), getDestName(), getAnchorText(), getNoFollow(), getFilePath(), getFileLocation());
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
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLinkIndri>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
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

            public ExtractedLinkIndri read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLinkIndri result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLinkIndri>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLinkIndri last = new ExtractedLinkIndri();         
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
            
            public final ExtractedLinkIndri read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                
                result.destUrl = buffer.getDestUrl();
                result.srcUrl = buffer.getSrcUrl();
                result.srcName = buffer.getSrcName();
                result.destName = buffer.getDestName();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                result.filePath = buffer.getFilePath();
                result.fileLocation = buffer.getFileLocation();
                
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
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean(), input.readString(), input.readLong());
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLinkIndri last = new ExtractedLinkIndri();
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
                               
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                processor.processTuple(srcUrl, srcName, destName, anchorText, noFollow, filePath, fileLocation);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLinkIndri last = new ExtractedLinkIndri();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor;                               
            
            public TupleUnshredder(ExtractedLinkIndri.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor) {
                this.processor = processor;
            }
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void processDestUrl(String destUrl) throws IOException {
                last.destUrl = destUrl;
            }   
                
            
            public void processTuple(String srcUrl, String srcName, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                last.srcUrl = srcUrl;
                last.srcName = srcName;
                last.destName = destName;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                last.filePath = filePath;
                last.fileLocation = fileLocation;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLinkIndri last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void process(ExtractedLinkIndri object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.destUrl, object.destUrl) != 0 || processAll) { processor.processDestUrl(object.destUrl); processAll = true; }
                processor.processTuple(object.srcUrl, object.srcName, object.destName, object.anchorText, object.noFollow, object.filePath, object.fileLocation);                                         
                last = object;
            }
                          
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class SrcUrlOrder implements Order<ExtractedLinkIndri> {
        public int hash(ExtractedLinkIndri object) {
            int h = 0;
            h += CmpUtil.hash(object.srcUrl);
            return h;
        } 
        public Comparator<ExtractedLinkIndri> greaterThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcUrl, two.srcUrl);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLinkIndri> lessThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcUrl, two.srcUrl);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLinkIndri> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLinkIndri > {
            ExtractedLinkIndri last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLinkIndri object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.srcUrl, last.srcUrl)) { processAll = true; shreddedWriter.processSrcUrl(object.srcUrl); }
               shreddedWriter.processTuple(object.srcName, object.destUrl, object.destName, object.anchorText, object.noFollow, object.filePath, object.fileLocation);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }
        } 
        public ReaderSource<ExtractedLinkIndri> orderedCombiner(Collection<TypeReader<ExtractedLinkIndri>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLinkIndri> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
            ExtractedLinkIndri result = new ExtractedLinkIndri();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            result.filePath = object.filePath; 
            result.fileLocation = object.fileLocation; 
            return result;
        }                 
        public Class<ExtractedLinkIndri> getOrderedClass() {
            return ExtractedLinkIndri.class;
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
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException;
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
            public final void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                if (lastFlush) {
                    if(buffer.srcUrls.size() == 0) buffer.processSrcUrl(lastSrcUrl);
                    lastFlush = false;
                }
                buffer.processTuple(srcName, destUrl, destName, anchorText, noFollow, filePath, fileLocation);
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
                    output.writeString(buffer.getFilePath());
                    output.writeLong(buffer.getFileLocation());
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
            String[] filePaths;
            long[] fileLocations;
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
                filePaths = new String[batchSize];
                fileLocations = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processSrcUrl(String srcUrl) {
                srcUrls.add(srcUrl);
                srcUrlTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) {
                assert srcUrls.size() > 0;
                srcNames[writeTupleIndex] = srcName;
                destUrls[writeTupleIndex] = destUrl;
                destNames[writeTupleIndex] = destName;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                filePaths[writeTupleIndex] = filePath;
                fileLocations[writeTupleIndex] = fileLocation;
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
            public String getFilePath() {
                assert readTupleIndex < writeTupleIndex;
                return filePaths[readTupleIndex];
            }                                         
            public long getFileLocation() {
                assert readTupleIndex < writeTupleIndex;
                return fileLocations[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcName(), getDestUrl(), getDestName(), getAnchorText(), getNoFollow(), getFilePath(), getFileLocation());
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
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLinkIndri>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
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

            public ExtractedLinkIndri read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLinkIndri result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLinkIndri>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLinkIndri last = new ExtractedLinkIndri();         
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
            
            public final ExtractedLinkIndri read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                
                result.srcUrl = buffer.getSrcUrl();
                result.srcName = buffer.getSrcName();
                result.destUrl = buffer.getDestUrl();
                result.destName = buffer.getDestName();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                result.filePath = buffer.getFilePath();
                result.fileLocation = buffer.getFileLocation();
                
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
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean(), input.readString(), input.readLong());
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLinkIndri last = new ExtractedLinkIndri();
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
                               
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                processor.processTuple(srcName, destUrl, destName, anchorText, noFollow, filePath, fileLocation);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLinkIndri last = new ExtractedLinkIndri();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor;                               
            
            public TupleUnshredder(ExtractedLinkIndri.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor) {
                this.processor = processor;
            }
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void processSrcUrl(String srcUrl) throws IOException {
                last.srcUrl = srcUrl;
            }   
                
            
            public void processTuple(String srcName, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                last.srcName = srcName;
                last.destUrl = destUrl;
                last.destName = destName;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                last.filePath = filePath;
                last.fileLocation = fileLocation;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLinkIndri last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void process(ExtractedLinkIndri object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.srcUrl, object.srcUrl) != 0 || processAll) { processor.processSrcUrl(object.srcUrl); processAll = true; }
                processor.processTuple(object.srcName, object.destUrl, object.destName, object.anchorText, object.noFollow, object.filePath, object.fileLocation);                                         
                last = object;
            }
                          
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class SrcNameOrder implements Order<ExtractedLinkIndri> {
        public int hash(ExtractedLinkIndri object) {
            int h = 0;
            h += CmpUtil.hash(object.srcName);
            return h;
        } 
        public Comparator<ExtractedLinkIndri> greaterThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcName, two.srcName);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLinkIndri> lessThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.srcName, two.srcName);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLinkIndri> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLinkIndri > {
            ExtractedLinkIndri last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLinkIndri object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.srcName, last.srcName)) { processAll = true; shreddedWriter.processSrcName(object.srcName); }
               shreddedWriter.processTuple(object.srcUrl, object.destUrl, object.destName, object.anchorText, object.noFollow, object.filePath, object.fileLocation);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }
        } 
        public ReaderSource<ExtractedLinkIndri> orderedCombiner(Collection<TypeReader<ExtractedLinkIndri>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLinkIndri> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
            ExtractedLinkIndri result = new ExtractedLinkIndri();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            result.filePath = object.filePath; 
            result.fileLocation = object.fileLocation; 
            return result;
        }                 
        public Class<ExtractedLinkIndri> getOrderedClass() {
            return ExtractedLinkIndri.class;
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
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException;
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
            public final void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                if (lastFlush) {
                    if(buffer.srcNames.size() == 0) buffer.processSrcName(lastSrcName);
                    lastFlush = false;
                }
                buffer.processTuple(srcUrl, destUrl, destName, anchorText, noFollow, filePath, fileLocation);
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
                    output.writeString(buffer.getFilePath());
                    output.writeLong(buffer.getFileLocation());
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
            String[] filePaths;
            long[] fileLocations;
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
                filePaths = new String[batchSize];
                fileLocations = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processSrcName(String srcName) {
                srcNames.add(srcName);
                srcNameTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) {
                assert srcNames.size() > 0;
                srcUrls[writeTupleIndex] = srcUrl;
                destUrls[writeTupleIndex] = destUrl;
                destNames[writeTupleIndex] = destName;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                filePaths[writeTupleIndex] = filePath;
                fileLocations[writeTupleIndex] = fileLocation;
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
            public String getFilePath() {
                assert readTupleIndex < writeTupleIndex;
                return filePaths[readTupleIndex];
            }                                         
            public long getFileLocation() {
                assert readTupleIndex < writeTupleIndex;
                return fileLocations[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcUrl(), getDestUrl(), getDestName(), getAnchorText(), getNoFollow(), getFilePath(), getFileLocation());
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
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLinkIndri>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
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

            public ExtractedLinkIndri read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLinkIndri result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLinkIndri>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLinkIndri last = new ExtractedLinkIndri();         
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
            
            public final ExtractedLinkIndri read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                
                result.srcName = buffer.getSrcName();
                result.srcUrl = buffer.getSrcUrl();
                result.destUrl = buffer.getDestUrl();
                result.destName = buffer.getDestName();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                result.filePath = buffer.getFilePath();
                result.fileLocation = buffer.getFileLocation();
                
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
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean(), input.readString(), input.readLong());
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLinkIndri last = new ExtractedLinkIndri();
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
                               
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                processor.processTuple(srcUrl, destUrl, destName, anchorText, noFollow, filePath, fileLocation);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLinkIndri last = new ExtractedLinkIndri();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor;                               
            
            public TupleUnshredder(ExtractedLinkIndri.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor) {
                this.processor = processor;
            }
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void processSrcName(String srcName) throws IOException {
                last.srcName = srcName;
            }   
                
            
            public void processTuple(String srcUrl, String destUrl, String destName, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                last.srcUrl = srcUrl;
                last.destUrl = destUrl;
                last.destName = destName;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                last.filePath = filePath;
                last.fileLocation = fileLocation;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLinkIndri last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void process(ExtractedLinkIndri object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.srcName, object.srcName) != 0 || processAll) { processor.processSrcName(object.srcName); processAll = true; }
                processor.processTuple(object.srcUrl, object.destUrl, object.destName, object.anchorText, object.noFollow, object.filePath, object.fileLocation);                                         
                last = object;
            }
                          
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class DestNameOrder implements Order<ExtractedLinkIndri> {
        public int hash(ExtractedLinkIndri object) {
            int h = 0;
            h += CmpUtil.hash(object.destName);
            return h;
        } 
        public Comparator<ExtractedLinkIndri> greaterThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destName, two.destName);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLinkIndri> lessThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.destName, two.destName);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLinkIndri> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLinkIndri > {
            ExtractedLinkIndri last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLinkIndri object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.destName, last.destName)) { processAll = true; shreddedWriter.processDestName(object.destName); }
               shreddedWriter.processTuple(object.srcUrl, object.srcName, object.destUrl, object.anchorText, object.noFollow, object.filePath, object.fileLocation);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }
        } 
        public ReaderSource<ExtractedLinkIndri> orderedCombiner(Collection<TypeReader<ExtractedLinkIndri>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLinkIndri> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
            ExtractedLinkIndri result = new ExtractedLinkIndri();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            result.filePath = object.filePath; 
            result.fileLocation = object.fileLocation; 
            return result;
        }                 
        public Class<ExtractedLinkIndri> getOrderedClass() {
            return ExtractedLinkIndri.class;
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
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException;
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
            public final void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                if (lastFlush) {
                    if(buffer.destNames.size() == 0) buffer.processDestName(lastDestName);
                    lastFlush = false;
                }
                buffer.processTuple(srcUrl, srcName, destUrl, anchorText, noFollow, filePath, fileLocation);
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
                    output.writeString(buffer.getFilePath());
                    output.writeLong(buffer.getFileLocation());
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
            String[] filePaths;
            long[] fileLocations;
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
                filePaths = new String[batchSize];
                fileLocations = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDestName(String destName) {
                destNames.add(destName);
                destNameTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow, String filePath, long fileLocation) {
                assert destNames.size() > 0;
                srcUrls[writeTupleIndex] = srcUrl;
                srcNames[writeTupleIndex] = srcName;
                destUrls[writeTupleIndex] = destUrl;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                filePaths[writeTupleIndex] = filePath;
                fileLocations[writeTupleIndex] = fileLocation;
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
            public String getFilePath() {
                assert readTupleIndex < writeTupleIndex;
                return filePaths[readTupleIndex];
            }                                         
            public long getFileLocation() {
                assert readTupleIndex < writeTupleIndex;
                return fileLocations[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getSrcUrl(), getSrcName(), getDestUrl(), getAnchorText(), getNoFollow(), getFilePath(), getFileLocation());
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
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLinkIndri>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
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

            public ExtractedLinkIndri read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLinkIndri result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLinkIndri>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLinkIndri last = new ExtractedLinkIndri();         
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
            
            public final ExtractedLinkIndri read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                
                result.destName = buffer.getDestName();
                result.srcUrl = buffer.getSrcUrl();
                result.srcName = buffer.getSrcName();
                result.destUrl = buffer.getDestUrl();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                result.filePath = buffer.getFilePath();
                result.fileLocation = buffer.getFileLocation();
                
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
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean(), input.readString(), input.readLong());
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLinkIndri last = new ExtractedLinkIndri();
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
                               
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                processor.processTuple(srcUrl, srcName, destUrl, anchorText, noFollow, filePath, fileLocation);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLinkIndri last = new ExtractedLinkIndri();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor;                               
            
            public TupleUnshredder(ExtractedLinkIndri.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor) {
                this.processor = processor;
            }
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void processDestName(String destName) throws IOException {
                last.destName = destName;
            }   
                
            
            public void processTuple(String srcUrl, String srcName, String destUrl, String anchorText, boolean noFollow, String filePath, long fileLocation) throws IOException {
                last.srcUrl = srcUrl;
                last.srcName = srcName;
                last.destUrl = destUrl;
                last.anchorText = anchorText;
                last.noFollow = noFollow;
                last.filePath = filePath;
                last.fileLocation = fileLocation;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            ExtractedLinkIndri last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void process(ExtractedLinkIndri object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.destName, object.destName) != 0 || processAll) { processor.processDestName(object.destName); processAll = true; }
                processor.processTuple(object.srcUrl, object.srcName, object.destUrl, object.anchorText, object.noFollow, object.filePath, object.fileLocation);                                         
                last = object;
            }
                          
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class FilePathFileLocationOrder implements Order<ExtractedLinkIndri> {
        public int hash(ExtractedLinkIndri object) {
            int h = 0;
            h += CmpUtil.hash(object.filePath);
            h += CmpUtil.hash(object.fileLocation);
            return h;
        } 
        public Comparator<ExtractedLinkIndri> greaterThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.filePath, two.filePath);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.fileLocation, two.fileLocation);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<ExtractedLinkIndri> lessThan() {
            return new Comparator<ExtractedLinkIndri>() {
                public int compare(ExtractedLinkIndri one, ExtractedLinkIndri two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.filePath, two.filePath);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.fileLocation, two.fileLocation);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<ExtractedLinkIndri> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<ExtractedLinkIndri> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< ExtractedLinkIndri > {
            ExtractedLinkIndri last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(ExtractedLinkIndri object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.filePath, last.filePath)) { processAll = true; shreddedWriter.processFilePath(object.filePath); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.fileLocation, last.fileLocation)) { processAll = true; shreddedWriter.processFileLocation(object.fileLocation); }
               shreddedWriter.processTuple(object.srcUrl, object.srcName, object.destUrl, object.destName, object.anchorText, object.noFollow);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }
        } 
        public ReaderSource<ExtractedLinkIndri> orderedCombiner(Collection<TypeReader<ExtractedLinkIndri>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<ExtractedLinkIndri> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
            ExtractedLinkIndri result = new ExtractedLinkIndri();
            if (object == null) return result;
            result.srcUrl = object.srcUrl; 
            result.srcName = object.srcName; 
            result.destUrl = object.destUrl; 
            result.destName = object.destName; 
            result.anchorText = object.anchorText; 
            result.noFollow = object.noFollow; 
            result.filePath = object.filePath; 
            result.fileLocation = object.fileLocation; 
            return result;
        }                 
        public Class<ExtractedLinkIndri> getOrderedClass() {
            return ExtractedLinkIndri.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+filePath", "+fileLocation"};
        }

        public static String[] getSpec() {
            return new String[] {"+filePath", "+fileLocation"};
        }
        public static String getSpecString() {
            return "+filePath +fileLocation";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processFilePath(String filePath) throws IOException;
            public void processFileLocation(long fileLocation) throws IOException;
            public void processTuple(String srcUrl, String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastFilePath;
            long lastFileLocation;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processFilePath(String filePath) {
                lastFilePath = filePath;
                buffer.processFilePath(filePath);
            }
            public void processFileLocation(long fileLocation) {
                lastFileLocation = fileLocation;
                buffer.processFileLocation(fileLocation);
            }
            public final void processTuple(String srcUrl, String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                if (lastFlush) {
                    if(buffer.filePaths.size() == 0) buffer.processFilePath(lastFilePath);
                    if(buffer.fileLocations.size() == 0) buffer.processFileLocation(lastFileLocation);
                    lastFlush = false;
                }
                buffer.processTuple(srcUrl, srcName, destUrl, destName, anchorText, noFollow);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getSrcUrl());
                    output.writeString(buffer.getSrcName());
                    output.writeString(buffer.getDestUrl());
                    output.writeString(buffer.getDestName());
                    output.writeString(buffer.getAnchorText());
                    output.writeBoolean(buffer.getNoFollow());
                    buffer.incrementTuple();
                }
            }  
            public final void flushFilePath(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getFilePathEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getFilePath());
                    output.writeInt(count);
                    buffer.incrementFilePath();
                      
                    flushFileLocation(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushFileLocation(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getFileLocationEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getFileLocation());
                    output.writeInt(count);
                    buffer.incrementFileLocation();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushFilePath(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> filePaths = new ArrayList<String>();
            TLongArrayList fileLocations = new TLongArrayList();
            TIntArrayList filePathTupleIdx = new TIntArrayList();
            TIntArrayList fileLocationTupleIdx = new TIntArrayList();
            int filePathReadIdx = 0;
            int fileLocationReadIdx = 0;
                            
            String[] srcUrls;
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

                srcUrls = new String[batchSize];
                srcNames = new String[batchSize];
                destUrls = new String[batchSize];
                destNames = new String[batchSize];
                anchorTexts = new String[batchSize];
                noFollows = new boolean[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processFilePath(String filePath) {
                filePaths.add(filePath);
                filePathTupleIdx.add(writeTupleIndex);
            }                                      
            public void processFileLocation(long fileLocation) {
                fileLocations.add(fileLocation);
                fileLocationTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String srcUrl, String srcName, String destUrl, String destName, String anchorText, boolean noFollow) {
                assert filePaths.size() > 0;
                assert fileLocations.size() > 0;
                srcUrls[writeTupleIndex] = srcUrl;
                srcNames[writeTupleIndex] = srcName;
                destUrls[writeTupleIndex] = destUrl;
                destNames[writeTupleIndex] = destName;
                anchorTexts[writeTupleIndex] = anchorText;
                noFollows[writeTupleIndex] = noFollow;
                writeTupleIndex++;
            }
            public void resetData() {
                filePaths.clear();
                fileLocations.clear();
                filePathTupleIdx.clear();
                fileLocationTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                filePathReadIdx = 0;
                fileLocationReadIdx = 0;
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
            public void incrementFilePath() {
                filePathReadIdx++;  
            }                                                                                              

            public void autoIncrementFilePath() {
                while (readTupleIndex >= getFilePathEndIndex() && readTupleIndex < writeTupleIndex)
                    filePathReadIdx++;
            }                 
            public void incrementFileLocation() {
                fileLocationReadIdx++;  
            }                                                                                              

            public void autoIncrementFileLocation() {
                while (readTupleIndex >= getFileLocationEndIndex() && readTupleIndex < writeTupleIndex)
                    fileLocationReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getFilePathEndIndex() {
                if ((filePathReadIdx+1) >= filePathTupleIdx.size())
                    return writeTupleIndex;
                return filePathTupleIdx.get(filePathReadIdx+1);
            }

            public int getFileLocationEndIndex() {
                if ((fileLocationReadIdx+1) >= fileLocationTupleIdx.size())
                    return writeTupleIndex;
                return fileLocationTupleIdx.get(fileLocationReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getFilePath() {
                assert readTupleIndex < writeTupleIndex;
                assert filePathReadIdx < filePaths.size();
                
                return filePaths.get(filePathReadIdx);
            }
            public long getFileLocation() {
                assert readTupleIndex < writeTupleIndex;
                assert fileLocationReadIdx < fileLocations.size();
                
                return fileLocations.get(fileLocationReadIdx);
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
                   output.processTuple(getSrcUrl(), getSrcName(), getDestUrl(), getDestName(), getAnchorText(), getNoFollow());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexFilePath(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processFilePath(getFilePath());
                    assert getFilePathEndIndex() <= endIndex;
                    copyUntilIndexFileLocation(getFilePathEndIndex(), output);
                    incrementFilePath();
                }
            } 
            public void copyUntilIndexFileLocation(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processFileLocation(getFileLocation());
                    assert getFileLocationEndIndex() <= endIndex;
                    copyTuples(getFileLocationEndIndex(), output);
                    incrementFileLocation();
                }
            }  
            public void copyUntilFilePath(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getFilePath(), other.getFilePath());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processFilePath(getFilePath());
                                      
                        if (c < 0) {
                            copyUntilIndexFileLocation(getFilePathEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilFileLocation(other, output);
                            autoIncrementFilePath();
                            break;
                        }
                    } else {
                        output.processFilePath(getFilePath());
                        copyUntilIndexFileLocation(getFilePathEndIndex(), output);
                    }
                    incrementFilePath();  
                    
               
                }
            }
            public void copyUntilFileLocation(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getFileLocation(), other.getFileLocation());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processFileLocation(getFileLocation());
                                      
                        copyTuples(getFileLocationEndIndex(), output);
                    } else {
                        output.processFileLocation(getFileLocation());
                        copyTuples(getFileLocationEndIndex(), output);
                    }
                    incrementFileLocation();  
                    
                    if (getFilePathEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilFilePath(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<ExtractedLinkIndri>, ShreddedSource {
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
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

            public ExtractedLinkIndri read() throws IOException {
                if (uninitialized)
                    initialize();

                ExtractedLinkIndri result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<ExtractedLinkIndri>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            ExtractedLinkIndri last = new ExtractedLinkIndri();         
            long updateFilePathCount = -1;
            long updateFileLocationCount = -1;
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
                    result = + CmpUtil.compare(buffer.getFilePath(), otherBuffer.getFilePath());
                    if(result != 0) break;
                    result = + CmpUtil.compare(buffer.getFileLocation(), otherBuffer.getFileLocation());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final ExtractedLinkIndri read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                
                result.filePath = buffer.getFilePath();
                result.fileLocation = buffer.getFileLocation();
                result.srcUrl = buffer.getSrcUrl();
                result.srcName = buffer.getSrcName();
                result.destUrl = buffer.getDestUrl();
                result.destName = buffer.getDestName();
                result.anchorText = buffer.getAnchorText();
                result.noFollow = buffer.getNoFollow();
                
                buffer.incrementTuple();
                buffer.autoIncrementFilePath();
                buffer.autoIncrementFileLocation();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateFilePathCount - tupleCount > 0) {
                            buffer.filePaths.add(last.filePath);
                            buffer.filePathTupleIdx.add((int) (updateFilePathCount - tupleCount));
                        }                              
                        if(updateFileLocationCount - tupleCount > 0) {
                            buffer.fileLocations.add(last.fileLocation);
                            buffer.fileLocationTupleIdx.add((int) (updateFileLocationCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateFileLocation();
                        buffer.processTuple(input.readString(), input.readString(), input.readString(), input.readString(), input.readString(), input.readBoolean());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateFilePath() throws IOException {
                if (updateFilePathCount > tupleCount)
                    return;
                     
                last.filePath = input.readString();
                updateFilePathCount = tupleCount + input.readInt();
                                      
                buffer.processFilePath(last.filePath);
            }
            public final void updateFileLocation() throws IOException {
                if (updateFileLocationCount > tupleCount)
                    return;
                     
                updateFilePath();
                last.fileLocation = input.readLong();
                updateFileLocationCount = tupleCount + input.readInt();
                                      
                buffer.processFileLocation(last.fileLocation);
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
                } else if (processor instanceof ExtractedLinkIndri.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((ExtractedLinkIndri.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<ExtractedLinkIndri> getOutputClass() {
                return ExtractedLinkIndri.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            ExtractedLinkIndri last = new ExtractedLinkIndri();
            boolean filePathProcess = true;
            boolean fileLocationProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processFilePath(String filePath) throws IOException {  
                if (filePathProcess || CmpUtil.compare(filePath, last.filePath) != 0) {
                    last.filePath = filePath;
                    processor.processFilePath(filePath);
            resetFileLocation();
                    filePathProcess = false;
                }
            }
            public void processFileLocation(long fileLocation) throws IOException {  
                if (fileLocationProcess || CmpUtil.compare(fileLocation, last.fileLocation) != 0) {
                    last.fileLocation = fileLocation;
                    processor.processFileLocation(fileLocation);
                    fileLocationProcess = false;
                }
            }  
            
            public void resetFilePath() {
                 filePathProcess = true;
            resetFileLocation();
            }                                                
            public void resetFileLocation() {
                 fileLocationProcess = true;
            }                                                
                               
            public void processTuple(String srcUrl, String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                processor.processTuple(srcUrl, srcName, destUrl, destName, anchorText, noFollow);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            ExtractedLinkIndri last = new ExtractedLinkIndri();
            public org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor;                               
            
            public TupleUnshredder(ExtractedLinkIndri.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<ExtractedLinkIndri> processor) {
                this.processor = processor;
            }
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void processFilePath(String filePath) throws IOException {
                last.filePath = filePath;
            }   
                
            public void processFileLocation(long fileLocation) throws IOException {
                last.fileLocation = fileLocation;
            }   
                
            
            public void processTuple(String srcUrl, String srcName, String destUrl, String destName, String anchorText, boolean noFollow) throws IOException {
                last.srcUrl = srcUrl;
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
            ExtractedLinkIndri last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public ExtractedLinkIndri clone(ExtractedLinkIndri object) {
                ExtractedLinkIndri result = new ExtractedLinkIndri();
                if (object == null) return result;
                result.srcUrl = object.srcUrl; 
                result.srcName = object.srcName; 
                result.destUrl = object.destUrl; 
                result.destName = object.destName; 
                result.anchorText = object.anchorText; 
                result.noFollow = object.noFollow; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void process(ExtractedLinkIndri object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.filePath, object.filePath) != 0 || processAll) { processor.processFilePath(object.filePath); processAll = true; }
                if(last == null || CmpUtil.compare(last.fileLocation, object.fileLocation) != 0 || processAll) { processor.processFileLocation(object.fileLocation); processAll = true; }
                processor.processTuple(object.srcUrl, object.srcName, object.destUrl, object.destName, object.anchorText, object.noFollow);                                         
                last = object;
            }
                          
            public Class<ExtractedLinkIndri> getInputClass() {
                return ExtractedLinkIndri.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    