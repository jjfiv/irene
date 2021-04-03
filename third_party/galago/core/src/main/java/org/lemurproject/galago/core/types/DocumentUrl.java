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
 * Tupleflow-Typebuilder automatically-generated class: DocumentUrl.
 */
@SuppressWarnings({"unused","unchecked"})
public final class DocumentUrl implements Type<DocumentUrl> {
    public String identifier;
    public String url;
    public String filePath;
    public long fileLocation; 
    
    /** default constructor makes most fields null */
    public DocumentUrl() {}
    /** additional constructor takes all fields explicitly */
    public DocumentUrl(String identifier, String url, String filePath, long fileLocation) {
        this.identifier = identifier;
        this.url = url;
        this.filePath = filePath;
        this.fileLocation = fileLocation;
    }  
    
    public String toString() {
            return String.format("%s,%s,%s,%d",
                                   identifier, url, filePath, fileLocation);
    } 

    public Order<DocumentUrl> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+url" })) {
            return new UrlOrder();
        }
        if (Arrays.equals(spec, new String[] { "+identifier" })) {
            return new IdentifierOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<DocumentUrl> {
        public void process(DocumentUrl object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class UrlOrder implements Order<DocumentUrl> {
        public int hash(DocumentUrl object) {
            int h = 0;
            h += CmpUtil.hash(object.url);
            return h;
        } 
        public Comparator<DocumentUrl> greaterThan() {
            return new Comparator<DocumentUrl>() {
                public int compare(DocumentUrl one, DocumentUrl two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.url, two.url);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentUrl> lessThan() {
            return new Comparator<DocumentUrl>() {
                public int compare(DocumentUrl one, DocumentUrl two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.url, two.url);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentUrl> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentUrl> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentUrl> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentUrl > {
            DocumentUrl last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentUrl object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.url, last.url)) { processAll = true; shreddedWriter.processUrl(object.url); }
               shreddedWriter.processTuple(object.identifier, object.filePath, object.fileLocation);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentUrl> getInputClass() {
                return DocumentUrl.class;
            }
        } 
        public ReaderSource<DocumentUrl> orderedCombiner(Collection<TypeReader<DocumentUrl>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentUrl> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentUrl clone(DocumentUrl object) {
            DocumentUrl result = new DocumentUrl();
            if (object == null) return result;
            result.identifier = object.identifier; 
            result.url = object.url; 
            result.filePath = object.filePath; 
            result.fileLocation = object.fileLocation; 
            return result;
        }                 
        public Class<DocumentUrl> getOrderedClass() {
            return DocumentUrl.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+url"};
        }

        public static String[] getSpec() {
            return new String[] {"+url"};
        }
        public static String getSpecString() {
            return "+url";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processUrl(String url) throws IOException;
            public void processTuple(String identifier, String filePath, long fileLocation) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastUrl;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processUrl(String url) {
                lastUrl = url;
                buffer.processUrl(url);
            }
            public final void processTuple(String identifier, String filePath, long fileLocation) throws IOException {
                if (lastFlush) {
                    if(buffer.urls.size() == 0) buffer.processUrl(lastUrl);
                    lastFlush = false;
                }
                buffer.processTuple(identifier, filePath, fileLocation);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getIdentifier());
                    output.writeString(buffer.getFilePath());
                    output.writeLong(buffer.getFileLocation());
                    buffer.incrementTuple();
                }
            }  
            public final void flushUrl(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getUrlEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getUrl());
                    output.writeInt(count);
                    buffer.incrementUrl();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushUrl(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> urls = new ArrayList<String>();
            TIntArrayList urlTupleIdx = new TIntArrayList();
            int urlReadIdx = 0;
                            
            String[] identifiers;
            String[] filePaths;
            long[] fileLocations;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                identifiers = new String[batchSize];
                filePaths = new String[batchSize];
                fileLocations = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processUrl(String url) {
                urls.add(url);
                urlTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String identifier, String filePath, long fileLocation) {
                assert urls.size() > 0;
                identifiers[writeTupleIndex] = identifier;
                filePaths[writeTupleIndex] = filePath;
                fileLocations[writeTupleIndex] = fileLocation;
                writeTupleIndex++;
            }
            public void resetData() {
                urls.clear();
                urlTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                urlReadIdx = 0;
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
            public void incrementUrl() {
                urlReadIdx++;  
            }                                                                                              

            public void autoIncrementUrl() {
                while (readTupleIndex >= getUrlEndIndex() && readTupleIndex < writeTupleIndex)
                    urlReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getUrlEndIndex() {
                if ((urlReadIdx+1) >= urlTupleIdx.size())
                    return writeTupleIndex;
                return urlTupleIdx.get(urlReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getUrl() {
                assert readTupleIndex < writeTupleIndex;
                assert urlReadIdx < urls.size();
                
                return urls.get(urlReadIdx);
            }
            public String getIdentifier() {
                assert readTupleIndex < writeTupleIndex;
                return identifiers[readTupleIndex];
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
                   output.processTuple(getIdentifier(), getFilePath(), getFileLocation());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexUrl(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processUrl(getUrl());
                    assert getUrlEndIndex() <= endIndex;
                    copyTuples(getUrlEndIndex(), output);
                    incrementUrl();
                }
            }  
            public void copyUntilUrl(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getUrl(), other.getUrl());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processUrl(getUrl());
                                      
                        copyTuples(getUrlEndIndex(), output);
                    } else {
                        output.processUrl(getUrl());
                        copyTuples(getUrlEndIndex(), output);
                    }
                    incrementUrl();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilUrl(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentUrl>, ShreddedSource {
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
                } else if (processor instanceof DocumentUrl.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentUrl.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentUrl>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentUrl> getOutputClass() {
                return DocumentUrl.class;
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

            public DocumentUrl read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentUrl result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentUrl>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentUrl last = new DocumentUrl();         
            long updateUrlCount = -1;
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
                    result = + CmpUtil.compare(buffer.getUrl(), otherBuffer.getUrl());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentUrl read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentUrl result = new DocumentUrl();
                
                result.url = buffer.getUrl();
                result.identifier = buffer.getIdentifier();
                result.filePath = buffer.getFilePath();
                result.fileLocation = buffer.getFileLocation();
                
                buffer.incrementTuple();
                buffer.autoIncrementUrl();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateUrlCount - tupleCount > 0) {
                            buffer.urls.add(last.url);
                            buffer.urlTupleIdx.add((int) (updateUrlCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateUrl();
                        buffer.processTuple(input.readString(), input.readString(), input.readLong());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateUrl() throws IOException {
                if (updateUrlCount > tupleCount)
                    return;
                     
                last.url = input.readString();
                updateUrlCount = tupleCount + input.readInt();
                                      
                buffer.processUrl(last.url);
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
                } else if (processor instanceof DocumentUrl.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentUrl.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentUrl>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentUrl> getOutputClass() {
                return DocumentUrl.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentUrl last = new DocumentUrl();
            boolean urlProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processUrl(String url) throws IOException {  
                if (urlProcess || CmpUtil.compare(url, last.url) != 0) {
                    last.url = url;
                    processor.processUrl(url);
                    urlProcess = false;
                }
            }  
            
            public void resetUrl() {
                 urlProcess = true;
            }                                                
                               
            public void processTuple(String identifier, String filePath, long fileLocation) throws IOException {
                processor.processTuple(identifier, filePath, fileLocation);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentUrl last = new DocumentUrl();
            public org.lemurproject.galago.tupleflow.Processor<DocumentUrl> processor;                               
            
            public TupleUnshredder(DocumentUrl.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentUrl> processor) {
                this.processor = processor;
            }
            
            public DocumentUrl clone(DocumentUrl object) {
                DocumentUrl result = new DocumentUrl();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void processUrl(String url) throws IOException {
                last.url = url;
            }   
                
            
            public void processTuple(String identifier, String filePath, long fileLocation) throws IOException {
                last.identifier = identifier;
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
            DocumentUrl last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentUrl clone(DocumentUrl object) {
                DocumentUrl result = new DocumentUrl();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void process(DocumentUrl object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.url, object.url) != 0 || processAll) { processor.processUrl(object.url); processAll = true; }
                processor.processTuple(object.identifier, object.filePath, object.fileLocation);                                         
                last = object;
            }
                          
            public Class<DocumentUrl> getInputClass() {
                return DocumentUrl.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class IdentifierOrder implements Order<DocumentUrl> {
        public int hash(DocumentUrl object) {
            int h = 0;
            h += CmpUtil.hash(object.identifier);
            return h;
        } 
        public Comparator<DocumentUrl> greaterThan() {
            return new Comparator<DocumentUrl>() {
                public int compare(DocumentUrl one, DocumentUrl two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.identifier, two.identifier);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentUrl> lessThan() {
            return new Comparator<DocumentUrl>() {
                public int compare(DocumentUrl one, DocumentUrl two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.identifier, two.identifier);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentUrl> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentUrl> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentUrl> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentUrl > {
            DocumentUrl last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentUrl object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.identifier, last.identifier)) { processAll = true; shreddedWriter.processIdentifier(object.identifier); }
               shreddedWriter.processTuple(object.url, object.filePath, object.fileLocation);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentUrl> getInputClass() {
                return DocumentUrl.class;
            }
        } 
        public ReaderSource<DocumentUrl> orderedCombiner(Collection<TypeReader<DocumentUrl>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentUrl> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentUrl clone(DocumentUrl object) {
            DocumentUrl result = new DocumentUrl();
            if (object == null) return result;
            result.identifier = object.identifier; 
            result.url = object.url; 
            result.filePath = object.filePath; 
            result.fileLocation = object.fileLocation; 
            return result;
        }                 
        public Class<DocumentUrl> getOrderedClass() {
            return DocumentUrl.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+identifier"};
        }

        public static String[] getSpec() {
            return new String[] {"+identifier"};
        }
        public static String getSpecString() {
            return "+identifier";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processIdentifier(String identifier) throws IOException;
            public void processTuple(String url, String filePath, long fileLocation) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastIdentifier;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processIdentifier(String identifier) {
                lastIdentifier = identifier;
                buffer.processIdentifier(identifier);
            }
            public final void processTuple(String url, String filePath, long fileLocation) throws IOException {
                if (lastFlush) {
                    if(buffer.identifiers.size() == 0) buffer.processIdentifier(lastIdentifier);
                    lastFlush = false;
                }
                buffer.processTuple(url, filePath, fileLocation);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getUrl());
                    output.writeString(buffer.getFilePath());
                    output.writeLong(buffer.getFileLocation());
                    buffer.incrementTuple();
                }
            }  
            public final void flushIdentifier(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getIdentifierEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getIdentifier());
                    output.writeInt(count);
                    buffer.incrementIdentifier();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushIdentifier(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> identifiers = new ArrayList<String>();
            TIntArrayList identifierTupleIdx = new TIntArrayList();
            int identifierReadIdx = 0;
                            
            String[] urls;
            String[] filePaths;
            long[] fileLocations;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                urls = new String[batchSize];
                filePaths = new String[batchSize];
                fileLocations = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processIdentifier(String identifier) {
                identifiers.add(identifier);
                identifierTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String url, String filePath, long fileLocation) {
                assert identifiers.size() > 0;
                urls[writeTupleIndex] = url;
                filePaths[writeTupleIndex] = filePath;
                fileLocations[writeTupleIndex] = fileLocation;
                writeTupleIndex++;
            }
            public void resetData() {
                identifiers.clear();
                identifierTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                identifierReadIdx = 0;
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
            public void incrementIdentifier() {
                identifierReadIdx++;  
            }                                                                                              

            public void autoIncrementIdentifier() {
                while (readTupleIndex >= getIdentifierEndIndex() && readTupleIndex < writeTupleIndex)
                    identifierReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getIdentifierEndIndex() {
                if ((identifierReadIdx+1) >= identifierTupleIdx.size())
                    return writeTupleIndex;
                return identifierTupleIdx.get(identifierReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getIdentifier() {
                assert readTupleIndex < writeTupleIndex;
                assert identifierReadIdx < identifiers.size();
                
                return identifiers.get(identifierReadIdx);
            }
            public String getUrl() {
                assert readTupleIndex < writeTupleIndex;
                return urls[readTupleIndex];
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
                   output.processTuple(getUrl(), getFilePath(), getFileLocation());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexIdentifier(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processIdentifier(getIdentifier());
                    assert getIdentifierEndIndex() <= endIndex;
                    copyTuples(getIdentifierEndIndex(), output);
                    incrementIdentifier();
                }
            }  
            public void copyUntilIdentifier(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getIdentifier(), other.getIdentifier());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processIdentifier(getIdentifier());
                                      
                        copyTuples(getIdentifierEndIndex(), output);
                    } else {
                        output.processIdentifier(getIdentifier());
                        copyTuples(getIdentifierEndIndex(), output);
                    }
                    incrementIdentifier();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilIdentifier(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentUrl>, ShreddedSource {
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
                } else if (processor instanceof DocumentUrl.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentUrl.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentUrl>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentUrl> getOutputClass() {
                return DocumentUrl.class;
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

            public DocumentUrl read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentUrl result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentUrl>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentUrl last = new DocumentUrl();         
            long updateIdentifierCount = -1;
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
                    result = + CmpUtil.compare(buffer.getIdentifier(), otherBuffer.getIdentifier());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentUrl read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentUrl result = new DocumentUrl();
                
                result.identifier = buffer.getIdentifier();
                result.url = buffer.getUrl();
                result.filePath = buffer.getFilePath();
                result.fileLocation = buffer.getFileLocation();
                
                buffer.incrementTuple();
                buffer.autoIncrementIdentifier();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateIdentifierCount - tupleCount > 0) {
                            buffer.identifiers.add(last.identifier);
                            buffer.identifierTupleIdx.add((int) (updateIdentifierCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateIdentifier();
                        buffer.processTuple(input.readString(), input.readString(), input.readLong());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateIdentifier() throws IOException {
                if (updateIdentifierCount > tupleCount)
                    return;
                     
                last.identifier = input.readString();
                updateIdentifierCount = tupleCount + input.readInt();
                                      
                buffer.processIdentifier(last.identifier);
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
                } else if (processor instanceof DocumentUrl.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentUrl.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentUrl>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentUrl> getOutputClass() {
                return DocumentUrl.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentUrl last = new DocumentUrl();
            boolean identifierProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processIdentifier(String identifier) throws IOException {  
                if (identifierProcess || CmpUtil.compare(identifier, last.identifier) != 0) {
                    last.identifier = identifier;
                    processor.processIdentifier(identifier);
                    identifierProcess = false;
                }
            }  
            
            public void resetIdentifier() {
                 identifierProcess = true;
            }                                                
                               
            public void processTuple(String url, String filePath, long fileLocation) throws IOException {
                processor.processTuple(url, filePath, fileLocation);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentUrl last = new DocumentUrl();
            public org.lemurproject.galago.tupleflow.Processor<DocumentUrl> processor;                               
            
            public TupleUnshredder(DocumentUrl.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentUrl> processor) {
                this.processor = processor;
            }
            
            public DocumentUrl clone(DocumentUrl object) {
                DocumentUrl result = new DocumentUrl();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void processIdentifier(String identifier) throws IOException {
                last.identifier = identifier;
            }   
                
            
            public void processTuple(String url, String filePath, long fileLocation) throws IOException {
                last.url = url;
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
            DocumentUrl last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentUrl clone(DocumentUrl object) {
                DocumentUrl result = new DocumentUrl();
                if (object == null) return result;
                result.identifier = object.identifier; 
                result.url = object.url; 
                result.filePath = object.filePath; 
                result.fileLocation = object.fileLocation; 
                return result;
            }                 
            
            public void process(DocumentUrl object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.identifier, object.identifier) != 0 || processAll) { processor.processIdentifier(object.identifier); processAll = true; }
                processor.processTuple(object.url, object.filePath, object.fileLocation);                                         
                last = object;
            }
                          
            public Class<DocumentUrl> getInputClass() {
                return DocumentUrl.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    