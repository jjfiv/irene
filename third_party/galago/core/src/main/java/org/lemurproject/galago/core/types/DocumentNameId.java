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
 * Tupleflow-Typebuilder automatically-generated class: DocumentNameId.
 */
@SuppressWarnings({"unused","unchecked"})
public final class DocumentNameId implements Type<DocumentNameId> {
    public byte[] name;
    public long id; 
    
    /** default constructor makes most fields null */
    public DocumentNameId() {}
    /** additional constructor takes all fields explicitly */
    public DocumentNameId(byte[] name, long id) {
        this.name = name;
        this.id = id;
    }  
    
    public String toString() {
        try {
            return String.format("%s,%d",
                                   new String(name, "UTF-8"), id);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException("Couldn't convert string to UTF-8.");
        }
    } 

    public Order<DocumentNameId> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+name" })) {
            return new NameOrder();
        }
        if (Arrays.equals(spec, new String[] { "+id" })) {
            return new IdOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<DocumentNameId> {
        public void process(DocumentNameId object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class NameOrder implements Order<DocumentNameId> {
        public int hash(DocumentNameId object) {
            int h = 0;
            h += CmpUtil.hash(object.name);
            return h;
        } 
        public Comparator<DocumentNameId> greaterThan() {
            return new Comparator<DocumentNameId>() {
                public int compare(DocumentNameId one, DocumentNameId two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.name, two.name);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentNameId> lessThan() {
            return new Comparator<DocumentNameId>() {
                public int compare(DocumentNameId one, DocumentNameId two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.name, two.name);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentNameId> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentNameId> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentNameId> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentNameId > {
            DocumentNameId last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentNameId object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.name, last.name)) { processAll = true; shreddedWriter.processName(object.name); }
               shreddedWriter.processTuple(object.id);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentNameId> getInputClass() {
                return DocumentNameId.class;
            }
        } 
        public ReaderSource<DocumentNameId> orderedCombiner(Collection<TypeReader<DocumentNameId>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentNameId> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentNameId clone(DocumentNameId object) {
            DocumentNameId result = new DocumentNameId();
            if (object == null) return result;
            result.name = object.name; 
            result.id = object.id; 
            return result;
        }                 
        public Class<DocumentNameId> getOrderedClass() {
            return DocumentNameId.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+name"};
        }

        public static String[] getSpec() {
            return new String[] {"+name"};
        }
        public static String getSpecString() {
            return "+name";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processName(byte[] name) throws IOException;
            public void processTuple(long id) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            byte[] lastName;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processName(byte[] name) {
                lastName = name;
                buffer.processName(name);
            }
            public final void processTuple(long id) throws IOException {
                if (lastFlush) {
                    if(buffer.names.size() == 0) buffer.processName(lastName);
                    lastFlush = false;
                }
                buffer.processTuple(id);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeLong(buffer.getId());
                    buffer.incrementTuple();
                }
            }  
            public final void flushName(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getNameEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeBytes(buffer.getName());
                    output.writeInt(count);
                    buffer.incrementName();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushName(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<byte[]> names = new ArrayList<byte[]>();
            TIntArrayList nameTupleIdx = new TIntArrayList();
            int nameReadIdx = 0;
                            
            long[] ids;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                ids = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processName(byte[] name) {
                names.add(name);
                nameTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(long id) {
                assert names.size() > 0;
                ids[writeTupleIndex] = id;
                writeTupleIndex++;
            }
            public void resetData() {
                names.clear();
                nameTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                nameReadIdx = 0;
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
            public void incrementName() {
                nameReadIdx++;  
            }                                                                                              

            public void autoIncrementName() {
                while (readTupleIndex >= getNameEndIndex() && readTupleIndex < writeTupleIndex)
                    nameReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getNameEndIndex() {
                if ((nameReadIdx+1) >= nameTupleIdx.size())
                    return writeTupleIndex;
                return nameTupleIdx.get(nameReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public byte[] getName() {
                assert readTupleIndex < writeTupleIndex;
                assert nameReadIdx < names.size();
                
                return names.get(nameReadIdx);
            }
            public long getId() {
                assert readTupleIndex < writeTupleIndex;
                return ids[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getId());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexName(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processName(getName());
                    assert getNameEndIndex() <= endIndex;
                    copyTuples(getNameEndIndex(), output);
                    incrementName();
                }
            }  
            public void copyUntilName(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getName(), other.getName());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processName(getName());
                                      
                        copyTuples(getNameEndIndex(), output);
                    } else {
                        output.processName(getName());
                        copyTuples(getNameEndIndex(), output);
                    }
                    incrementName();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilName(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentNameId>, ShreddedSource {
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
                } else if (processor instanceof DocumentNameId.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentNameId.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentNameId>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentNameId> getOutputClass() {
                return DocumentNameId.class;
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

            public DocumentNameId read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentNameId result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentNameId>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentNameId last = new DocumentNameId();         
            long updateNameCount = -1;
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
                    result = + CmpUtil.compare(buffer.getName(), otherBuffer.getName());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentNameId read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentNameId result = new DocumentNameId();
                
                result.name = buffer.getName();
                result.id = buffer.getId();
                
                buffer.incrementTuple();
                buffer.autoIncrementName();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateNameCount - tupleCount > 0) {
                            buffer.names.add(last.name);
                            buffer.nameTupleIdx.add((int) (updateNameCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateName();
                        buffer.processTuple(input.readLong());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateName() throws IOException {
                if (updateNameCount > tupleCount)
                    return;
                     
                last.name = input.readBytes();
                updateNameCount = tupleCount + input.readInt();
                                      
                buffer.processName(last.name);
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
                } else if (processor instanceof DocumentNameId.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentNameId.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentNameId>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentNameId> getOutputClass() {
                return DocumentNameId.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentNameId last = new DocumentNameId();
            boolean nameProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processName(byte[] name) throws IOException {  
                if (nameProcess || CmpUtil.compare(name, last.name) != 0) {
                    last.name = name;
                    processor.processName(name);
                    nameProcess = false;
                }
            }  
            
            public void resetName() {
                 nameProcess = true;
            }                                                
                               
            public void processTuple(long id) throws IOException {
                processor.processTuple(id);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentNameId last = new DocumentNameId();
            public org.lemurproject.galago.tupleflow.Processor<DocumentNameId> processor;                               
            
            public TupleUnshredder(DocumentNameId.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentNameId> processor) {
                this.processor = processor;
            }
            
            public DocumentNameId clone(DocumentNameId object) {
                DocumentNameId result = new DocumentNameId();
                if (object == null) return result;
                result.name = object.name; 
                result.id = object.id; 
                return result;
            }                 
            
            public void processName(byte[] name) throws IOException {
                last.name = name;
            }   
                
            
            public void processTuple(long id) throws IOException {
                last.id = id;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            DocumentNameId last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentNameId clone(DocumentNameId object) {
                DocumentNameId result = new DocumentNameId();
                if (object == null) return result;
                result.name = object.name; 
                result.id = object.id; 
                return result;
            }                 
            
            public void process(DocumentNameId object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.name, object.name) != 0 || processAll) { processor.processName(object.name); processAll = true; }
                processor.processTuple(object.id);                                         
                last = object;
            }
                          
            public Class<DocumentNameId> getInputClass() {
                return DocumentNameId.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class IdOrder implements Order<DocumentNameId> {
        public int hash(DocumentNameId object) {
            int h = 0;
            h += CmpUtil.hash(object.id);
            return h;
        } 
        public Comparator<DocumentNameId> greaterThan() {
            return new Comparator<DocumentNameId>() {
                public int compare(DocumentNameId one, DocumentNameId two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.id, two.id);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentNameId> lessThan() {
            return new Comparator<DocumentNameId>() {
                public int compare(DocumentNameId one, DocumentNameId two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.id, two.id);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentNameId> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentNameId> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentNameId> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< DocumentNameId > {
            DocumentNameId last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentNameId object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.id, last.id)) { processAll = true; shreddedWriter.processId(object.id); }
               shreddedWriter.processTuple(object.name);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentNameId> getInputClass() {
                return DocumentNameId.class;
            }
        } 
        public ReaderSource<DocumentNameId> orderedCombiner(Collection<TypeReader<DocumentNameId>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<DocumentNameId> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentNameId clone(DocumentNameId object) {
            DocumentNameId result = new DocumentNameId();
            if (object == null) return result;
            result.name = object.name; 
            result.id = object.id; 
            return result;
        }                 
        public Class<DocumentNameId> getOrderedClass() {
            return DocumentNameId.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+id"};
        }

        public static String[] getSpec() {
            return new String[] {"+id"};
        }
        public static String getSpecString() {
            return "+id";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processId(long id) throws IOException;
            public void processTuple(byte[] name) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            long lastId;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processId(long id) {
                lastId = id;
                buffer.processId(id);
            }
            public final void processTuple(byte[] name) throws IOException {
                if (lastFlush) {
                    if(buffer.ids.size() == 0) buffer.processId(lastId);
                    lastFlush = false;
                }
                buffer.processTuple(name);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeBytes(buffer.getName());
                    buffer.incrementTuple();
                }
            }  
            public final void flushId(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getIdEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getId());
                    output.writeInt(count);
                    buffer.incrementId();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushId(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TLongArrayList ids = new TLongArrayList();
            TIntArrayList idTupleIdx = new TIntArrayList();
            int idReadIdx = 0;
                            
            byte[][] names;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                names = new byte[batchSize][];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processId(long id) {
                ids.add(id);
                idTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(byte[] name) {
                assert ids.size() > 0;
                names[writeTupleIndex] = name;
                writeTupleIndex++;
            }
            public void resetData() {
                ids.clear();
                idTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                idReadIdx = 0;
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
            public void incrementId() {
                idReadIdx++;  
            }                                                                                              

            public void autoIncrementId() {
                while (readTupleIndex >= getIdEndIndex() && readTupleIndex < writeTupleIndex)
                    idReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getIdEndIndex() {
                if ((idReadIdx+1) >= idTupleIdx.size())
                    return writeTupleIndex;
                return idTupleIdx.get(idReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public long getId() {
                assert readTupleIndex < writeTupleIndex;
                assert idReadIdx < ids.size();
                
                return ids.get(idReadIdx);
            }
            public byte[] getName() {
                assert readTupleIndex < writeTupleIndex;
                return names[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getName());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexId(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processId(getId());
                    assert getIdEndIndex() <= endIndex;
                    copyTuples(getIdEndIndex(), output);
                    incrementId();
                }
            }  
            public void copyUntilId(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getId(), other.getId());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processId(getId());
                                      
                        copyTuples(getIdEndIndex(), output);
                    } else {
                        output.processId(getId());
                        copyTuples(getIdEndIndex(), output);
                    }
                    incrementId();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilId(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<DocumentNameId>, ShreddedSource {
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
                } else if (processor instanceof DocumentNameId.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentNameId.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentNameId>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentNameId> getOutputClass() {
                return DocumentNameId.class;
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

            public DocumentNameId read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentNameId result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentNameId>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentNameId last = new DocumentNameId();         
            long updateIdCount = -1;
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
                    result = + CmpUtil.compare(buffer.getId(), otherBuffer.getId());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentNameId read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentNameId result = new DocumentNameId();
                
                result.id = buffer.getId();
                result.name = buffer.getName();
                
                buffer.incrementTuple();
                buffer.autoIncrementId();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateIdCount - tupleCount > 0) {
                            buffer.ids.add(last.id);
                            buffer.idTupleIdx.add((int) (updateIdCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateId();
                        buffer.processTuple(input.readBytes());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateId() throws IOException {
                if (updateIdCount > tupleCount)
                    return;
                     
                last.id = input.readLong();
                updateIdCount = tupleCount + input.readInt();
                                      
                buffer.processId(last.id);
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
                } else if (processor instanceof DocumentNameId.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentNameId.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<DocumentNameId>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<DocumentNameId> getOutputClass() {
                return DocumentNameId.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentNameId last = new DocumentNameId();
            boolean idProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processId(long id) throws IOException {  
                if (idProcess || CmpUtil.compare(id, last.id) != 0) {
                    last.id = id;
                    processor.processId(id);
                    idProcess = false;
                }
            }  
            
            public void resetId() {
                 idProcess = true;
            }                                                
                               
            public void processTuple(byte[] name) throws IOException {
                processor.processTuple(name);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            DocumentNameId last = new DocumentNameId();
            public org.lemurproject.galago.tupleflow.Processor<DocumentNameId> processor;                               
            
            public TupleUnshredder(DocumentNameId.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<DocumentNameId> processor) {
                this.processor = processor;
            }
            
            public DocumentNameId clone(DocumentNameId object) {
                DocumentNameId result = new DocumentNameId();
                if (object == null) return result;
                result.name = object.name; 
                result.id = object.id; 
                return result;
            }                 
            
            public void processId(long id) throws IOException {
                last.id = id;
            }   
                
            
            public void processTuple(byte[] name) throws IOException {
                last.name = name;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            DocumentNameId last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentNameId clone(DocumentNameId object) {
                DocumentNameId result = new DocumentNameId();
                if (object == null) return result;
                result.name = object.name; 
                result.id = object.id; 
                return result;
            }                 
            
            public void process(DocumentNameId object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.id, object.id) != 0 || processAll) { processor.processId(object.id); processAll = true; }
                processor.processTuple(object.name);                                         
                last = object;
            }
                          
            public Class<DocumentNameId> getInputClass() {
                return DocumentNameId.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    