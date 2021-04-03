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
 * Tupleflow-Typebuilder automatically-generated class: DocumentProbability.
 */
@SuppressWarnings({"unused","unchecked"})
public final class DocumentProbability implements Type<DocumentProbability> {
    public String document;
    public double probability; 
    
    /** default constructor makes most fields null */
    public DocumentProbability() {}
    /** additional constructor takes all fields explicitly */
    public DocumentProbability(String document, double probability) {
        this.document = document;
        this.probability = probability;
    }  
    
    public String toString() {
            return String.format("%s,%g",
                                   document, probability);
    } 

    public Order<DocumentProbability> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+document" })) {
            return new DocumentOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<DocumentProbability> {
        public void process(DocumentProbability object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class DocumentOrder implements Order<DocumentProbability> {
        public int hash(DocumentProbability object) {
            int h = 0;
            h += CmpUtil.hash(object.document);
            return h;
        } 
        public Comparator<DocumentProbability> greaterThan() {
            return new Comparator<DocumentProbability>() {
                public int compare(DocumentProbability one, DocumentProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.document, two.document);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentProbability> lessThan() {
            return new Comparator<DocumentProbability>() {
                public int compare(DocumentProbability one, DocumentProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.document, two.document);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentProbability> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentProbability> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentProbability> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentProbability > {
            DocumentProbability last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentProbability object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.document, last.document)) { processAll = true; shreddedWriter.processDocument(object.document); }
               shreddedWriter.processTuple(object.probability);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentProbability> getInputClass() {
                return DocumentProbability.class;
            }
        } 
        public ReaderSource<DocumentProbability> orderedCombiner(Collection<TypeReader<DocumentProbability>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentProbability> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentProbability clone(DocumentProbability object) {
            DocumentProbability result = new DocumentProbability();
            if (object == null) return result;
            result.document = object.document; 
            result.probability = object.probability; 
            return result;
        }                 
        public Class<DocumentProbability> getOrderedClass() {
            return DocumentProbability.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+document"};
        }

        public static String[] getSpec() {
            return new String[] {"+document"};
        }
        public static String getSpecString() {
            return "+document";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processDocument(String document) throws IOException;
            public void processTuple(double probability) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastDocument;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processDocument(String document) {
                lastDocument = document;
                buffer.processDocument(document);
            }
            public final void processTuple(double probability) throws IOException {
                if (lastFlush) {
                    if(buffer.documents.size() == 0) buffer.processDocument(lastDocument);
                    lastFlush = false;
                }
                buffer.processTuple(probability);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeDouble(buffer.getProbability());
                    buffer.incrementTuple();
                }
            }  
            public final void flushDocument(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getDocumentEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getDocument());
                    output.writeInt(count);
                    buffer.incrementDocument();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushDocument(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<String> documents = new ArrayList<String>();
            TIntArrayList documentTupleIdx = new TIntArrayList();
            int documentReadIdx = 0;
                            
            double[] probabilitys;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                probabilitys = new double[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processDocument(String document) {
                documents.add(document);
                documentTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(double probability) {
                assert documents.size() > 0;
                probabilitys[writeTupleIndex] = probability;
                writeTupleIndex++;
            }
            public void resetData() {
                documents.clear();
                documentTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                documentReadIdx = 0;
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
            public void incrementDocument() {
                documentReadIdx++;  
            }                                                                                              

            public void autoIncrementDocument() {
                while (readTupleIndex >= getDocumentEndIndex() && readTupleIndex < writeTupleIndex)
                    documentReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getDocumentEndIndex() {
                if ((documentReadIdx+1) >= documentTupleIdx.size())
                    return writeTupleIndex;
                return documentTupleIdx.get(documentReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getDocument() {
                assert readTupleIndex < writeTupleIndex;
                assert documentReadIdx < documents.size();
                
                return documents.get(documentReadIdx);
            }
            public double getProbability() {
                assert readTupleIndex < writeTupleIndex;
                return probabilitys[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getProbability());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexDocument(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processDocument(getDocument());
                    assert getDocumentEndIndex() <= endIndex;
                    copyTuples(getDocumentEndIndex(), output);
                    incrementDocument();
                }
            }  
            public void copyUntilDocument(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getDocument(), other.getDocument());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processDocument(getDocument());
                                      
                        copyTuples(getDocumentEndIndex(), output);
                    } else {
                        output.processDocument(getDocument());
                        copyTuples(getDocumentEndIndex(), output);
                    }
                    incrementDocument();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilDocument(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentProbability>, ShreddedSource {
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
                } else if (processor instanceof DocumentProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentProbability> getOutputClass() {
                return DocumentProbability.class;
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

            public DocumentProbability read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentProbability result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentProbability>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentProbability last = new DocumentProbability();         
            long updateDocumentCount = -1;
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
                    result = + CmpUtil.compare(buffer.getDocument(), otherBuffer.getDocument());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentProbability read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentProbability result = new DocumentProbability();
                
                result.document = buffer.getDocument();
                result.probability = buffer.getProbability();
                
                buffer.incrementTuple();
                buffer.autoIncrementDocument();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateDocumentCount - tupleCount > 0) {
                            buffer.documents.add(last.document);
                            buffer.documentTupleIdx.add((int) (updateDocumentCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateDocument();
                        buffer.processTuple(input.readDouble());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateDocument() throws IOException {
                if (updateDocumentCount > tupleCount)
                    return;
                     
                last.document = input.readString();
                updateDocumentCount = tupleCount + input.readInt();
                                      
                buffer.processDocument(last.document);
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
                } else if (processor instanceof DocumentProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentProbability> getOutputClass() {
                return DocumentProbability.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentProbability last = new DocumentProbability();
            boolean documentProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processDocument(String document) throws IOException {  
                if (documentProcess || CmpUtil.compare(document, last.document) != 0) {
                    last.document = document;
                    processor.processDocument(document);
                    documentProcess = false;
                }
            }  
            
            public void resetDocument() {
                 documentProcess = true;
            }                                                
                               
            public void processTuple(double probability) throws IOException {
                processor.processTuple(probability);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentProbability last = new DocumentProbability();
            public org.lemurproject.galago.tupleflow.Processor<DocumentProbability> processor;                               
            
            public TupleUnshredder(DocumentProbability.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentProbability> processor) {
                this.processor = processor;
            }
            
            public DocumentProbability clone(DocumentProbability object) {
                DocumentProbability result = new DocumentProbability();
                if (object == null) return result;
                result.document = object.document; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void processDocument(String document) throws IOException {
                last.document = document;
            }   
                
            
            public void processTuple(double probability) throws IOException {
                last.probability = probability;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            DocumentProbability last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentProbability clone(DocumentProbability object) {
                DocumentProbability result = new DocumentProbability();
                if (object == null) return result;
                result.document = object.document; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void process(DocumentProbability object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.document, object.document) != 0 || processAll) { processor.processDocument(object.document); processAll = true; }
                processor.processTuple(object.probability);                                         
                last = object;
            }
                          
            public Class<DocumentProbability> getInputClass() {
                return DocumentProbability.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    