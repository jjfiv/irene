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
 * Tupleflow-Typebuilder automatically-generated class: NumberWordProbability.
 */
@SuppressWarnings({"unused","unchecked"})
public final class NumberWordProbability implements Type<NumberWordProbability> {
    public long number;
    public byte[] word;
    public double probability; 
    
    /** default constructor makes most fields null */
    public NumberWordProbability() {}
    /** additional constructor takes all fields explicitly */
    public NumberWordProbability(long number, byte[] word, double probability) {
        this.number = number;
        this.word = word;
        this.probability = probability;
    }  
    
    public String toString() {
        try {
            return String.format("%d,%s,%g",
                                   number, new String(word, "UTF-8"), probability);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException("Couldn't convert string to UTF-8.");
        }
    } 

    public Order<NumberWordProbability> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+number", "+word" })) {
            return new NumberWordOrder();
        }
        if (Arrays.equals(spec, new String[] { "+word", "-probability", "+number" })) {
            return new WordDescProbabilityNumberOrder();
        }
        if (Arrays.equals(spec, new String[] { "-probability", "+number" })) {
            return new DescProbabilityNumberOrder();
        }
        if (Arrays.equals(spec, new String[] { "+number" })) {
            return new NumberOrder();
        }
        if (Arrays.equals(spec, new String[] { "+word" })) {
            return new WordOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> {
        public void process(NumberWordProbability object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class NumberWordOrder implements Order<NumberWordProbability> {
        public int hash(NumberWordProbability object) {
            int h = 0;
            h += CmpUtil.hash(object.number);
            h += CmpUtil.hash(object.word);
            return h;
        } 
        public Comparator<NumberWordProbability> greaterThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberWordProbability> lessThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberWordProbability> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< NumberWordProbability > {
            NumberWordProbability last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberWordProbability object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.number, last.number)) { processAll = true; shreddedWriter.processNumber(object.number); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.word, last.word)) { processAll = true; shreddedWriter.processWord(object.word); }
               shreddedWriter.processTuple(object.probability);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }
        } 
        public ReaderSource<NumberWordProbability> orderedCombiner(Collection<TypeReader<NumberWordProbability>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<NumberWordProbability> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberWordProbability clone(NumberWordProbability object) {
            NumberWordProbability result = new NumberWordProbability();
            if (object == null) return result;
            result.number = object.number; 
            result.word = object.word; 
            result.probability = object.probability; 
            return result;
        }                 
        public Class<NumberWordProbability> getOrderedClass() {
            return NumberWordProbability.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+number", "+word"};
        }

        public static String[] getSpec() {
            return new String[] {"+number", "+word"};
        }
        public static String getSpecString() {
            return "+number +word";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processNumber(long number) throws IOException;
            public void processWord(byte[] word) throws IOException;
            public void processTuple(double probability) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            long lastNumber;
            byte[] lastWord;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processNumber(long number) {
                lastNumber = number;
                buffer.processNumber(number);
            }
            public void processWord(byte[] word) {
                lastWord = word;
                buffer.processWord(word);
            }
            public final void processTuple(double probability) throws IOException {
                if (lastFlush) {
                    if(buffer.numbers.size() == 0) buffer.processNumber(lastNumber);
                    if(buffer.words.size() == 0) buffer.processWord(lastWord);
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
            public final void flushNumber(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getNumberEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getNumber());
                    output.writeInt(count);
                    buffer.incrementNumber();
                      
                    flushWord(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushWord(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getWordEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeBytes(buffer.getWord());
                    output.writeInt(count);
                    buffer.incrementWord();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushNumber(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TLongArrayList numbers = new TLongArrayList();
            ArrayList<byte[]> words = new ArrayList<byte[]>();
            TIntArrayList numberTupleIdx = new TIntArrayList();
            TIntArrayList wordTupleIdx = new TIntArrayList();
            int numberReadIdx = 0;
            int wordReadIdx = 0;
                            
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
            
            public void processNumber(long number) {
                numbers.add(number);
                numberTupleIdx.add(writeTupleIndex);
            }                                      
            public void processWord(byte[] word) {
                words.add(word);
                wordTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(double probability) {
                assert numbers.size() > 0;
                assert words.size() > 0;
                probabilitys[writeTupleIndex] = probability;
                writeTupleIndex++;
            }
            public void resetData() {
                numbers.clear();
                words.clear();
                numberTupleIdx.clear();
                wordTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                numberReadIdx = 0;
                wordReadIdx = 0;
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
            public void incrementNumber() {
                numberReadIdx++;  
            }                                                                                              

            public void autoIncrementNumber() {
                while (readTupleIndex >= getNumberEndIndex() && readTupleIndex < writeTupleIndex)
                    numberReadIdx++;
            }                 
            public void incrementWord() {
                wordReadIdx++;  
            }                                                                                              

            public void autoIncrementWord() {
                while (readTupleIndex >= getWordEndIndex() && readTupleIndex < writeTupleIndex)
                    wordReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getNumberEndIndex() {
                if ((numberReadIdx+1) >= numberTupleIdx.size())
                    return writeTupleIndex;
                return numberTupleIdx.get(numberReadIdx+1);
            }

            public int getWordEndIndex() {
                if ((wordReadIdx+1) >= wordTupleIdx.size())
                    return writeTupleIndex;
                return wordTupleIdx.get(wordReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public long getNumber() {
                assert readTupleIndex < writeTupleIndex;
                assert numberReadIdx < numbers.size();
                
                return numbers.get(numberReadIdx);
            }
            public byte[] getWord() {
                assert readTupleIndex < writeTupleIndex;
                assert wordReadIdx < words.size();
                
                return words.get(wordReadIdx);
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
            public void copyUntilIndexNumber(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processNumber(getNumber());
                    assert getNumberEndIndex() <= endIndex;
                    copyUntilIndexWord(getNumberEndIndex(), output);
                    incrementNumber();
                }
            } 
            public void copyUntilIndexWord(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processWord(getWord());
                    assert getWordEndIndex() <= endIndex;
                    copyTuples(getWordEndIndex(), output);
                    incrementWord();
                }
            }  
            public void copyUntilNumber(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getNumber(), other.getNumber());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processNumber(getNumber());
                                      
                        if (c < 0) {
                            copyUntilIndexWord(getNumberEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilWord(other, output);
                            autoIncrementNumber();
                            break;
                        }
                    } else {
                        output.processNumber(getNumber());
                        copyUntilIndexWord(getNumberEndIndex(), output);
                    }
                    incrementNumber();  
                    
               
                }
            }
            public void copyUntilWord(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getWord(), other.getWord());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processWord(getWord());
                                      
                        copyTuples(getWordEndIndex(), output);
                    } else {
                        output.processWord(getWord());
                        copyTuples(getWordEndIndex(), output);
                    }
                    incrementWord();  
                    
                    if (getNumberEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilNumber(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<NumberWordProbability>, ShreddedSource {
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
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

            public NumberWordProbability read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberWordProbability result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberWordProbability>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberWordProbability last = new NumberWordProbability();         
            long updateNumberCount = -1;
            long updateWordCount = -1;
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
                    result = + CmpUtil.compare(buffer.getNumber(), otherBuffer.getNumber());
                    if(result != 0) break;
                    result = + CmpUtil.compare(buffer.getWord(), otherBuffer.getWord());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberWordProbability read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberWordProbability result = new NumberWordProbability();
                
                result.number = buffer.getNumber();
                result.word = buffer.getWord();
                result.probability = buffer.getProbability();
                
                buffer.incrementTuple();
                buffer.autoIncrementNumber();
                buffer.autoIncrementWord();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateNumberCount - tupleCount > 0) {
                            buffer.numbers.add(last.number);
                            buffer.numberTupleIdx.add((int) (updateNumberCount - tupleCount));
                        }                              
                        if(updateWordCount - tupleCount > 0) {
                            buffer.words.add(last.word);
                            buffer.wordTupleIdx.add((int) (updateWordCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateWord();
                        buffer.processTuple(input.readDouble());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateNumber() throws IOException {
                if (updateNumberCount > tupleCount)
                    return;
                     
                last.number = input.readLong();
                updateNumberCount = tupleCount + input.readInt();
                                      
                buffer.processNumber(last.number);
            }
            public final void updateWord() throws IOException {
                if (updateWordCount > tupleCount)
                    return;
                     
                updateNumber();
                last.word = input.readBytes();
                updateWordCount = tupleCount + input.readInt();
                                      
                buffer.processWord(last.word);
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberWordProbability last = new NumberWordProbability();
            boolean numberProcess = true;
            boolean wordProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processNumber(long number) throws IOException {  
                if (numberProcess || CmpUtil.compare(number, last.number) != 0) {
                    last.number = number;
                    processor.processNumber(number);
            resetWord();
                    numberProcess = false;
                }
            }
            public void processWord(byte[] word) throws IOException {  
                if (wordProcess || CmpUtil.compare(word, last.word) != 0) {
                    last.word = word;
                    processor.processWord(word);
                    wordProcess = false;
                }
            }  
            
            public void resetNumber() {
                 numberProcess = true;
            resetWord();
            }                                                
            public void resetWord() {
                 wordProcess = true;
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
            NumberWordProbability last = new NumberWordProbability();
            public org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor;                               
            
            public TupleUnshredder(NumberWordProbability.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor) {
                this.processor = processor;
            }
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void processNumber(long number) throws IOException {
                last.number = number;
            }   
                
            public void processWord(byte[] word) throws IOException {
                last.word = word;
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
            NumberWordProbability last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void process(NumberWordProbability object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.number, object.number) != 0 || processAll) { processor.processNumber(object.number); processAll = true; }
                if(last == null || CmpUtil.compare(last.word, object.word) != 0 || processAll) { processor.processWord(object.word); processAll = true; }
                processor.processTuple(object.probability);                                         
                last = object;
            }
                          
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class WordDescProbabilityNumberOrder implements Order<NumberWordProbability> {
        public int hash(NumberWordProbability object) {
            int h = 0;
            h += CmpUtil.hash(object.word);
            h += CmpUtil.hash(object.probability);
            h += CmpUtil.hash(object.number);
            return h;
        } 
        public Comparator<NumberWordProbability> greaterThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                        result = - CmpUtil.compare(one.probability, two.probability);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberWordProbability> lessThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                        result = - CmpUtil.compare(one.probability, two.probability);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberWordProbability> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< NumberWordProbability > {
            NumberWordProbability last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberWordProbability object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.word, last.word)) { processAll = true; shreddedWriter.processWord(object.word); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.probability, last.probability)) { processAll = true; shreddedWriter.processProbability(object.probability); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.number, last.number)) { processAll = true; shreddedWriter.processNumber(object.number); }
               shreddedWriter.processTuple();
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }
        } 
        public ReaderSource<NumberWordProbability> orderedCombiner(Collection<TypeReader<NumberWordProbability>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<NumberWordProbability> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberWordProbability clone(NumberWordProbability object) {
            NumberWordProbability result = new NumberWordProbability();
            if (object == null) return result;
            result.number = object.number; 
            result.word = object.word; 
            result.probability = object.probability; 
            return result;
        }                 
        public Class<NumberWordProbability> getOrderedClass() {
            return NumberWordProbability.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+word", "-probability", "+number"};
        }

        public static String[] getSpec() {
            return new String[] {"+word", "-probability", "+number"};
        }
        public static String getSpecString() {
            return "+word -probability +number";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processWord(byte[] word) throws IOException;
            public void processProbability(double probability) throws IOException;
            public void processNumber(long number) throws IOException;
            public void processTuple() throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            byte[] lastWord;
            double lastProbability;
            long lastNumber;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processWord(byte[] word) {
                lastWord = word;
                buffer.processWord(word);
            }
            public void processProbability(double probability) {
                lastProbability = probability;
                buffer.processProbability(probability);
            }
            public void processNumber(long number) {
                lastNumber = number;
                buffer.processNumber(number);
            }
            public final void processTuple() throws IOException {
                if (lastFlush) {
                    if(buffer.words.size() == 0) buffer.processWord(lastWord);
                    if(buffer.probabilitys.size() == 0) buffer.processProbability(lastProbability);
                    if(buffer.numbers.size() == 0) buffer.processNumber(lastNumber);
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
            public final void flushWord(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getWordEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeBytes(buffer.getWord());
                    output.writeInt(count);
                    buffer.incrementWord();
                      
                    flushProbability(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushProbability(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getProbabilityEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeDouble(buffer.getProbability());
                    output.writeInt(count);
                    buffer.incrementProbability();
                      
                    flushNumber(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushNumber(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getNumberEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getNumber());
                    output.writeInt(count);
                    buffer.incrementNumber();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushWord(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<byte[]> words = new ArrayList<byte[]>();
            TDoubleArrayList probabilitys = new TDoubleArrayList();
            TLongArrayList numbers = new TLongArrayList();
            TIntArrayList wordTupleIdx = new TIntArrayList();
            TIntArrayList probabilityTupleIdx = new TIntArrayList();
            TIntArrayList numberTupleIdx = new TIntArrayList();
            int wordReadIdx = 0;
            int probabilityReadIdx = 0;
            int numberReadIdx = 0;
                            
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processWord(byte[] word) {
                words.add(word);
                wordTupleIdx.add(writeTupleIndex);
            }                                      
            public void processProbability(double probability) {
                probabilitys.add(probability);
                probabilityTupleIdx.add(writeTupleIndex);
            }                                      
            public void processNumber(long number) {
                numbers.add(number);
                numberTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple() {
                assert words.size() > 0;
                assert probabilitys.size() > 0;
                assert numbers.size() > 0;
                writeTupleIndex++;
            }
            public void resetData() {
                words.clear();
                probabilitys.clear();
                numbers.clear();
                wordTupleIdx.clear();
                probabilityTupleIdx.clear();
                numberTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                wordReadIdx = 0;
                probabilityReadIdx = 0;
                numberReadIdx = 0;
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
            public void incrementWord() {
                wordReadIdx++;  
            }                                                                                              

            public void autoIncrementWord() {
                while (readTupleIndex >= getWordEndIndex() && readTupleIndex < writeTupleIndex)
                    wordReadIdx++;
            }                 
            public void incrementProbability() {
                probabilityReadIdx++;  
            }                                                                                              

            public void autoIncrementProbability() {
                while (readTupleIndex >= getProbabilityEndIndex() && readTupleIndex < writeTupleIndex)
                    probabilityReadIdx++;
            }                 
            public void incrementNumber() {
                numberReadIdx++;  
            }                                                                                              

            public void autoIncrementNumber() {
                while (readTupleIndex >= getNumberEndIndex() && readTupleIndex < writeTupleIndex)
                    numberReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getWordEndIndex() {
                if ((wordReadIdx+1) >= wordTupleIdx.size())
                    return writeTupleIndex;
                return wordTupleIdx.get(wordReadIdx+1);
            }

            public int getProbabilityEndIndex() {
                if ((probabilityReadIdx+1) >= probabilityTupleIdx.size())
                    return writeTupleIndex;
                return probabilityTupleIdx.get(probabilityReadIdx+1);
            }

            public int getNumberEndIndex() {
                if ((numberReadIdx+1) >= numberTupleIdx.size())
                    return writeTupleIndex;
                return numberTupleIdx.get(numberReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public byte[] getWord() {
                assert readTupleIndex < writeTupleIndex;
                assert wordReadIdx < words.size();
                
                return words.get(wordReadIdx);
            }
            public double getProbability() {
                assert readTupleIndex < writeTupleIndex;
                assert probabilityReadIdx < probabilitys.size();
                
                return probabilitys.get(probabilityReadIdx);
            }
            public long getNumber() {
                assert readTupleIndex < writeTupleIndex;
                assert numberReadIdx < numbers.size();
                
                return numbers.get(numberReadIdx);
            }

            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple();
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexWord(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processWord(getWord());
                    assert getWordEndIndex() <= endIndex;
                    copyUntilIndexProbability(getWordEndIndex(), output);
                    incrementWord();
                }
            } 
            public void copyUntilIndexProbability(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processProbability(getProbability());
                    assert getProbabilityEndIndex() <= endIndex;
                    copyUntilIndexNumber(getProbabilityEndIndex(), output);
                    incrementProbability();
                }
            } 
            public void copyUntilIndexNumber(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processNumber(getNumber());
                    assert getNumberEndIndex() <= endIndex;
                    copyTuples(getNumberEndIndex(), output);
                    incrementNumber();
                }
            }  
            public void copyUntilWord(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getWord(), other.getWord());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processWord(getWord());
                                      
                        if (c < 0) {
                            copyUntilIndexProbability(getWordEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilProbability(other, output);
                            autoIncrementWord();
                            break;
                        }
                    } else {
                        output.processWord(getWord());
                        copyUntilIndexProbability(getWordEndIndex(), output);
                    }
                    incrementWord();  
                    
               
                }
            }
            public void copyUntilProbability(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = - CmpUtil.compare(getProbability(), other.getProbability());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processProbability(getProbability());
                                      
                        if (c < 0) {
                            copyUntilIndexNumber(getProbabilityEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilNumber(other, output);
                            autoIncrementProbability();
                            break;
                        }
                    } else {
                        output.processProbability(getProbability());
                        copyUntilIndexNumber(getProbabilityEndIndex(), output);
                    }
                    incrementProbability();  
                    
                    if (getWordEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntilNumber(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getNumber(), other.getNumber());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processNumber(getNumber());
                                      
                        copyTuples(getNumberEndIndex(), output);
                    } else {
                        output.processNumber(getNumber());
                        copyTuples(getNumberEndIndex(), output);
                    }
                    incrementNumber();  
                    
                    if (getProbabilityEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilWord(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<NumberWordProbability>, ShreddedSource {
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
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

            public NumberWordProbability read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberWordProbability result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberWordProbability>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberWordProbability last = new NumberWordProbability();         
            long updateWordCount = -1;
            long updateProbabilityCount = -1;
            long updateNumberCount = -1;
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
                    result = + CmpUtil.compare(buffer.getWord(), otherBuffer.getWord());
                    if(result != 0) break;
                    result = - CmpUtil.compare(buffer.getProbability(), otherBuffer.getProbability());
                    if(result != 0) break;
                    result = + CmpUtil.compare(buffer.getNumber(), otherBuffer.getNumber());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberWordProbability read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberWordProbability result = new NumberWordProbability();
                
                result.word = buffer.getWord();
                result.probability = buffer.getProbability();
                result.number = buffer.getNumber();
                
                buffer.incrementTuple();
                buffer.autoIncrementWord();
                buffer.autoIncrementProbability();
                buffer.autoIncrementNumber();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateWordCount - tupleCount > 0) {
                            buffer.words.add(last.word);
                            buffer.wordTupleIdx.add((int) (updateWordCount - tupleCount));
                        }                              
                        if(updateProbabilityCount - tupleCount > 0) {
                            buffer.probabilitys.add(last.probability);
                            buffer.probabilityTupleIdx.add((int) (updateProbabilityCount - tupleCount));
                        }                              
                        if(updateNumberCount - tupleCount > 0) {
                            buffer.numbers.add(last.number);
                            buffer.numberTupleIdx.add((int) (updateNumberCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateNumber();
                        buffer.processTuple();
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateWord() throws IOException {
                if (updateWordCount > tupleCount)
                    return;
                     
                last.word = input.readBytes();
                updateWordCount = tupleCount + input.readInt();
                                      
                buffer.processWord(last.word);
            }
            public final void updateProbability() throws IOException {
                if (updateProbabilityCount > tupleCount)
                    return;
                     
                updateWord();
                last.probability = input.readDouble();
                updateProbabilityCount = tupleCount + input.readInt();
                                      
                buffer.processProbability(last.probability);
            }
            public final void updateNumber() throws IOException {
                if (updateNumberCount > tupleCount)
                    return;
                     
                updateProbability();
                last.number = input.readLong();
                updateNumberCount = tupleCount + input.readInt();
                                      
                buffer.processNumber(last.number);
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberWordProbability last = new NumberWordProbability();
            boolean wordProcess = true;
            boolean probabilityProcess = true;
            boolean numberProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processWord(byte[] word) throws IOException {  
                if (wordProcess || CmpUtil.compare(word, last.word) != 0) {
                    last.word = word;
                    processor.processWord(word);
            resetProbability();
                    wordProcess = false;
                }
            }
            public void processProbability(double probability) throws IOException {  
                if (probabilityProcess || CmpUtil.compare(probability, last.probability) != 0) {
                    last.probability = probability;
                    processor.processProbability(probability);
            resetNumber();
                    probabilityProcess = false;
                }
            }
            public void processNumber(long number) throws IOException {  
                if (numberProcess || CmpUtil.compare(number, last.number) != 0) {
                    last.number = number;
                    processor.processNumber(number);
                    numberProcess = false;
                }
            }  
            
            public void resetWord() {
                 wordProcess = true;
            resetProbability();
            }                                                
            public void resetProbability() {
                 probabilityProcess = true;
            resetNumber();
            }                                                
            public void resetNumber() {
                 numberProcess = true;
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
            NumberWordProbability last = new NumberWordProbability();
            public org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor;                               
            
            public TupleUnshredder(NumberWordProbability.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor) {
                this.processor = processor;
            }
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void processWord(byte[] word) throws IOException {
                last.word = word;
            }   
                
            public void processProbability(double probability) throws IOException {
                last.probability = probability;
            }   
                
            public void processNumber(long number) throws IOException {
                last.number = number;
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
            NumberWordProbability last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void process(NumberWordProbability object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.word, object.word) != 0 || processAll) { processor.processWord(object.word); processAll = true; }
                if(last == null || CmpUtil.compare(last.probability, object.probability) != 0 || processAll) { processor.processProbability(object.probability); processAll = true; }
                if(last == null || CmpUtil.compare(last.number, object.number) != 0 || processAll) { processor.processNumber(object.number); processAll = true; }
                processor.processTuple();                                         
                last = object;
            }
                          
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class DescProbabilityNumberOrder implements Order<NumberWordProbability> {
        public int hash(NumberWordProbability object) {
            int h = 0;
            h += CmpUtil.hash(object.probability);
            h += CmpUtil.hash(object.number);
            return h;
        } 
        public Comparator<NumberWordProbability> greaterThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = - CmpUtil.compare(one.probability, two.probability);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberWordProbability> lessThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = - CmpUtil.compare(one.probability, two.probability);
                        if(result != 0) break;
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberWordProbability> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< NumberWordProbability > {
            NumberWordProbability last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberWordProbability object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.probability, last.probability)) { processAll = true; shreddedWriter.processProbability(object.probability); }
               if (processAll || last == null || 0 != CmpUtil.compare(object.number, last.number)) { processAll = true; shreddedWriter.processNumber(object.number); }
               shreddedWriter.processTuple(object.word);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }
        } 
        public ReaderSource<NumberWordProbability> orderedCombiner(Collection<TypeReader<NumberWordProbability>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<NumberWordProbability> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberWordProbability clone(NumberWordProbability object) {
            NumberWordProbability result = new NumberWordProbability();
            if (object == null) return result;
            result.number = object.number; 
            result.word = object.word; 
            result.probability = object.probability; 
            return result;
        }                 
        public Class<NumberWordProbability> getOrderedClass() {
            return NumberWordProbability.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"-probability", "+number"};
        }

        public static String[] getSpec() {
            return new String[] {"-probability", "+number"};
        }
        public static String getSpecString() {
            return "-probability +number";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processProbability(double probability) throws IOException;
            public void processNumber(long number) throws IOException;
            public void processTuple(byte[] word) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            double lastProbability;
            long lastNumber;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processProbability(double probability) {
                lastProbability = probability;
                buffer.processProbability(probability);
            }
            public void processNumber(long number) {
                lastNumber = number;
                buffer.processNumber(number);
            }
            public final void processTuple(byte[] word) throws IOException {
                if (lastFlush) {
                    if(buffer.probabilitys.size() == 0) buffer.processProbability(lastProbability);
                    if(buffer.numbers.size() == 0) buffer.processNumber(lastNumber);
                    lastFlush = false;
                }
                buffer.processTuple(word);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeBytes(buffer.getWord());
                    buffer.incrementTuple();
                }
            }  
            public final void flushProbability(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getProbabilityEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeDouble(buffer.getProbability());
                    output.writeInt(count);
                    buffer.incrementProbability();
                      
                    flushNumber(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public final void flushNumber(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getNumberEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getNumber());
                    output.writeInt(count);
                    buffer.incrementNumber();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushProbability(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TDoubleArrayList probabilitys = new TDoubleArrayList();
            TLongArrayList numbers = new TLongArrayList();
            TIntArrayList probabilityTupleIdx = new TIntArrayList();
            TIntArrayList numberTupleIdx = new TIntArrayList();
            int probabilityReadIdx = 0;
            int numberReadIdx = 0;
                            
            byte[][] words;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                words = new byte[batchSize][];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processProbability(double probability) {
                probabilitys.add(probability);
                probabilityTupleIdx.add(writeTupleIndex);
            }                                      
            public void processNumber(long number) {
                numbers.add(number);
                numberTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(byte[] word) {
                assert probabilitys.size() > 0;
                assert numbers.size() > 0;
                words[writeTupleIndex] = word;
                writeTupleIndex++;
            }
            public void resetData() {
                probabilitys.clear();
                numbers.clear();
                probabilityTupleIdx.clear();
                numberTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                probabilityReadIdx = 0;
                numberReadIdx = 0;
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
            public void incrementProbability() {
                probabilityReadIdx++;  
            }                                                                                              

            public void autoIncrementProbability() {
                while (readTupleIndex >= getProbabilityEndIndex() && readTupleIndex < writeTupleIndex)
                    probabilityReadIdx++;
            }                 
            public void incrementNumber() {
                numberReadIdx++;  
            }                                                                                              

            public void autoIncrementNumber() {
                while (readTupleIndex >= getNumberEndIndex() && readTupleIndex < writeTupleIndex)
                    numberReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getProbabilityEndIndex() {
                if ((probabilityReadIdx+1) >= probabilityTupleIdx.size())
                    return writeTupleIndex;
                return probabilityTupleIdx.get(probabilityReadIdx+1);
            }

            public int getNumberEndIndex() {
                if ((numberReadIdx+1) >= numberTupleIdx.size())
                    return writeTupleIndex;
                return numberTupleIdx.get(numberReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public double getProbability() {
                assert readTupleIndex < writeTupleIndex;
                assert probabilityReadIdx < probabilitys.size();
                
                return probabilitys.get(probabilityReadIdx);
            }
            public long getNumber() {
                assert readTupleIndex < writeTupleIndex;
                assert numberReadIdx < numbers.size();
                
                return numbers.get(numberReadIdx);
            }
            public byte[] getWord() {
                assert readTupleIndex < writeTupleIndex;
                return words[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getWord());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexProbability(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processProbability(getProbability());
                    assert getProbabilityEndIndex() <= endIndex;
                    copyUntilIndexNumber(getProbabilityEndIndex(), output);
                    incrementProbability();
                }
            } 
            public void copyUntilIndexNumber(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processNumber(getNumber());
                    assert getNumberEndIndex() <= endIndex;
                    copyTuples(getNumberEndIndex(), output);
                    incrementNumber();
                }
            }  
            public void copyUntilProbability(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = - CmpUtil.compare(getProbability(), other.getProbability());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processProbability(getProbability());
                                      
                        if (c < 0) {
                            copyUntilIndexNumber(getProbabilityEndIndex(), output);
                        } else if (c == 0) {
                            copyUntilNumber(other, output);
                            autoIncrementProbability();
                            break;
                        }
                    } else {
                        output.processProbability(getProbability());
                        copyUntilIndexNumber(getProbabilityEndIndex(), output);
                    }
                    incrementProbability();  
                    
               
                }
            }
            public void copyUntilNumber(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getNumber(), other.getNumber());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processNumber(getNumber());
                                      
                        copyTuples(getNumberEndIndex(), output);
                    } else {
                        output.processNumber(getNumber());
                        copyTuples(getNumberEndIndex(), output);
                    }
                    incrementNumber();  
                    
                    if (getProbabilityEndIndex() <= readTupleIndex)
                        break;   
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilProbability(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<NumberWordProbability>, ShreddedSource {
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
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

            public NumberWordProbability read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberWordProbability result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberWordProbability>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberWordProbability last = new NumberWordProbability();         
            long updateProbabilityCount = -1;
            long updateNumberCount = -1;
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
                    result = - CmpUtil.compare(buffer.getProbability(), otherBuffer.getProbability());
                    if(result != 0) break;
                    result = + CmpUtil.compare(buffer.getNumber(), otherBuffer.getNumber());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberWordProbability read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberWordProbability result = new NumberWordProbability();
                
                result.probability = buffer.getProbability();
                result.number = buffer.getNumber();
                result.word = buffer.getWord();
                
                buffer.incrementTuple();
                buffer.autoIncrementProbability();
                buffer.autoIncrementNumber();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateProbabilityCount - tupleCount > 0) {
                            buffer.probabilitys.add(last.probability);
                            buffer.probabilityTupleIdx.add((int) (updateProbabilityCount - tupleCount));
                        }                              
                        if(updateNumberCount - tupleCount > 0) {
                            buffer.numbers.add(last.number);
                            buffer.numberTupleIdx.add((int) (updateNumberCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateNumber();
                        buffer.processTuple(input.readBytes());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateProbability() throws IOException {
                if (updateProbabilityCount > tupleCount)
                    return;
                     
                last.probability = input.readDouble();
                updateProbabilityCount = tupleCount + input.readInt();
                                      
                buffer.processProbability(last.probability);
            }
            public final void updateNumber() throws IOException {
                if (updateNumberCount > tupleCount)
                    return;
                     
                updateProbability();
                last.number = input.readLong();
                updateNumberCount = tupleCount + input.readInt();
                                      
                buffer.processNumber(last.number);
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberWordProbability last = new NumberWordProbability();
            boolean probabilityProcess = true;
            boolean numberProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processProbability(double probability) throws IOException {  
                if (probabilityProcess || CmpUtil.compare(probability, last.probability) != 0) {
                    last.probability = probability;
                    processor.processProbability(probability);
            resetNumber();
                    probabilityProcess = false;
                }
            }
            public void processNumber(long number) throws IOException {  
                if (numberProcess || CmpUtil.compare(number, last.number) != 0) {
                    last.number = number;
                    processor.processNumber(number);
                    numberProcess = false;
                }
            }  
            
            public void resetProbability() {
                 probabilityProcess = true;
            resetNumber();
            }                                                
            public void resetNumber() {
                 numberProcess = true;
            }                                                
                               
            public void processTuple(byte[] word) throws IOException {
                processor.processTuple(word);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            NumberWordProbability last = new NumberWordProbability();
            public org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor;                               
            
            public TupleUnshredder(NumberWordProbability.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor) {
                this.processor = processor;
            }
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void processProbability(double probability) throws IOException {
                last.probability = probability;
            }   
                
            public void processNumber(long number) throws IOException {
                last.number = number;
            }   
                
            
            public void processTuple(byte[] word) throws IOException {
                last.word = word;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            NumberWordProbability last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void process(NumberWordProbability object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.probability, object.probability) != 0 || processAll) { processor.processProbability(object.probability); processAll = true; }
                if(last == null || CmpUtil.compare(last.number, object.number) != 0 || processAll) { processor.processNumber(object.number); processAll = true; }
                processor.processTuple(object.word);                                         
                last = object;
            }
                          
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class NumberOrder implements Order<NumberWordProbability> {
        public int hash(NumberWordProbability object) {
            int h = 0;
            h += CmpUtil.hash(object.number);
            return h;
        } 
        public Comparator<NumberWordProbability> greaterThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberWordProbability> lessThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.number, two.number);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberWordProbability> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< NumberWordProbability > {
            NumberWordProbability last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberWordProbability object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.number, last.number)) { processAll = true; shreddedWriter.processNumber(object.number); }
               shreddedWriter.processTuple(object.word, object.probability);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }
        } 
        public ReaderSource<NumberWordProbability> orderedCombiner(Collection<TypeReader<NumberWordProbability>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<NumberWordProbability> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberWordProbability clone(NumberWordProbability object) {
            NumberWordProbability result = new NumberWordProbability();
            if (object == null) return result;
            result.number = object.number; 
            result.word = object.word; 
            result.probability = object.probability; 
            return result;
        }                 
        public Class<NumberWordProbability> getOrderedClass() {
            return NumberWordProbability.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+number"};
        }

        public static String[] getSpec() {
            return new String[] {"+number"};
        }
        public static String getSpecString() {
            return "+number";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processNumber(long number) throws IOException;
            public void processTuple(byte[] word, double probability) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            long lastNumber;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processNumber(long number) {
                lastNumber = number;
                buffer.processNumber(number);
            }
            public final void processTuple(byte[] word, double probability) throws IOException {
                if (lastFlush) {
                    if(buffer.numbers.size() == 0) buffer.processNumber(lastNumber);
                    lastFlush = false;
                }
                buffer.processTuple(word, probability);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeBytes(buffer.getWord());
                    output.writeDouble(buffer.getProbability());
                    buffer.incrementTuple();
                }
            }  
            public final void flushNumber(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getNumberEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeLong(buffer.getNumber());
                    output.writeInt(count);
                    buffer.incrementNumber();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushNumber(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            TLongArrayList numbers = new TLongArrayList();
            TIntArrayList numberTupleIdx = new TIntArrayList();
            int numberReadIdx = 0;
                            
            byte[][] words;
            double[] probabilitys;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                words = new byte[batchSize][];
                probabilitys = new double[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processNumber(long number) {
                numbers.add(number);
                numberTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(byte[] word, double probability) {
                assert numbers.size() > 0;
                words[writeTupleIndex] = word;
                probabilitys[writeTupleIndex] = probability;
                writeTupleIndex++;
            }
            public void resetData() {
                numbers.clear();
                numberTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                numberReadIdx = 0;
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
            public void incrementNumber() {
                numberReadIdx++;  
            }                                                                                              

            public void autoIncrementNumber() {
                while (readTupleIndex >= getNumberEndIndex() && readTupleIndex < writeTupleIndex)
                    numberReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getNumberEndIndex() {
                if ((numberReadIdx+1) >= numberTupleIdx.size())
                    return writeTupleIndex;
                return numberTupleIdx.get(numberReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public long getNumber() {
                assert readTupleIndex < writeTupleIndex;
                assert numberReadIdx < numbers.size();
                
                return numbers.get(numberReadIdx);
            }
            public byte[] getWord() {
                assert readTupleIndex < writeTupleIndex;
                return words[readTupleIndex];
            }                                         
            public double getProbability() {
                assert readTupleIndex < writeTupleIndex;
                return probabilitys[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getWord(), getProbability());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexNumber(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processNumber(getNumber());
                    assert getNumberEndIndex() <= endIndex;
                    copyTuples(getNumberEndIndex(), output);
                    incrementNumber();
                }
            }  
            public void copyUntilNumber(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getNumber(), other.getNumber());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processNumber(getNumber());
                                      
                        copyTuples(getNumberEndIndex(), output);
                    } else {
                        output.processNumber(getNumber());
                        copyTuples(getNumberEndIndex(), output);
                    }
                    incrementNumber();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilNumber(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<NumberWordProbability>, ShreddedSource {
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
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

            public NumberWordProbability read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberWordProbability result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberWordProbability>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberWordProbability last = new NumberWordProbability();         
            long updateNumberCount = -1;
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
                    result = + CmpUtil.compare(buffer.getNumber(), otherBuffer.getNumber());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberWordProbability read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberWordProbability result = new NumberWordProbability();
                
                result.number = buffer.getNumber();
                result.word = buffer.getWord();
                result.probability = buffer.getProbability();
                
                buffer.incrementTuple();
                buffer.autoIncrementNumber();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateNumberCount - tupleCount > 0) {
                            buffer.numbers.add(last.number);
                            buffer.numberTupleIdx.add((int) (updateNumberCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateNumber();
                        buffer.processTuple(input.readBytes(), input.readDouble());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateNumber() throws IOException {
                if (updateNumberCount > tupleCount)
                    return;
                     
                last.number = input.readLong();
                updateNumberCount = tupleCount + input.readInt();
                                      
                buffer.processNumber(last.number);
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberWordProbability last = new NumberWordProbability();
            boolean numberProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processNumber(long number) throws IOException {  
                if (numberProcess || CmpUtil.compare(number, last.number) != 0) {
                    last.number = number;
                    processor.processNumber(number);
                    numberProcess = false;
                }
            }  
            
            public void resetNumber() {
                 numberProcess = true;
            }                                                
                               
            public void processTuple(byte[] word, double probability) throws IOException {
                processor.processTuple(word, probability);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            NumberWordProbability last = new NumberWordProbability();
            public org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor;                               
            
            public TupleUnshredder(NumberWordProbability.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor) {
                this.processor = processor;
            }
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void processNumber(long number) throws IOException {
                last.number = number;
            }   
                
            
            public void processTuple(byte[] word, double probability) throws IOException {
                last.word = word;
                last.probability = probability;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            NumberWordProbability last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void process(NumberWordProbability object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.number, object.number) != 0 || processAll) { processor.processNumber(object.number); processAll = true; }
                processor.processTuple(object.word, object.probability);                                         
                last = object;
            }
                          
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
    public static final class WordOrder implements Order<NumberWordProbability> {
        public int hash(NumberWordProbability object) {
            int h = 0;
            h += CmpUtil.hash(object.word);
            return h;
        } 
        public Comparator<NumberWordProbability> greaterThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<NumberWordProbability> lessThan() {
            return new Comparator<NumberWordProbability>() {
                public int compare(NumberWordProbability one, NumberWordProbability two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<NumberWordProbability> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<NumberWordProbability> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< NumberWordProbability > {
            NumberWordProbability last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(NumberWordProbability object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.word, last.word)) { processAll = true; shreddedWriter.processWord(object.word); }
               shreddedWriter.processTuple(object.number, object.probability);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }
        } 
        public ReaderSource<NumberWordProbability> orderedCombiner(Collection<TypeReader<NumberWordProbability>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<NumberWordProbability> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public NumberWordProbability clone(NumberWordProbability object) {
            NumberWordProbability result = new NumberWordProbability();
            if (object == null) return result;
            result.number = object.number; 
            result.word = object.word; 
            result.probability = object.probability; 
            return result;
        }                 
        public Class<NumberWordProbability> getOrderedClass() {
            return NumberWordProbability.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+word"};
        }

        public static String[] getSpec() {
            return new String[] {"+word"};
        }
        public static String getSpecString() {
            return "+word";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processWord(byte[] word) throws IOException;
            public void processTuple(long number, double probability) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            byte[] lastWord;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processWord(byte[] word) {
                lastWord = word;
                buffer.processWord(word);
            }
            public final void processTuple(long number, double probability) throws IOException {
                if (lastFlush) {
                    if(buffer.words.size() == 0) buffer.processWord(lastWord);
                    lastFlush = false;
                }
                buffer.processTuple(number, probability);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeLong(buffer.getNumber());
                    output.writeDouble(buffer.getProbability());
                    buffer.incrementTuple();
                }
            }  
            public final void flushWord(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getWordEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeBytes(buffer.getWord());
                    output.writeInt(count);
                    buffer.incrementWord();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushWord(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<byte[]> words = new ArrayList<byte[]>();
            TIntArrayList wordTupleIdx = new TIntArrayList();
            int wordReadIdx = 0;
                            
            long[] numbers;
            double[] probabilitys;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                numbers = new long[batchSize];
                probabilitys = new double[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processWord(byte[] word) {
                words.add(word);
                wordTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(long number, double probability) {
                assert words.size() > 0;
                numbers[writeTupleIndex] = number;
                probabilitys[writeTupleIndex] = probability;
                writeTupleIndex++;
            }
            public void resetData() {
                words.clear();
                wordTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                wordReadIdx = 0;
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
            public void incrementWord() {
                wordReadIdx++;  
            }                                                                                              

            public void autoIncrementWord() {
                while (readTupleIndex >= getWordEndIndex() && readTupleIndex < writeTupleIndex)
                    wordReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getWordEndIndex() {
                if ((wordReadIdx+1) >= wordTupleIdx.size())
                    return writeTupleIndex;
                return wordTupleIdx.get(wordReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public byte[] getWord() {
                assert readTupleIndex < writeTupleIndex;
                assert wordReadIdx < words.size();
                
                return words.get(wordReadIdx);
            }
            public long getNumber() {
                assert readTupleIndex < writeTupleIndex;
                return numbers[readTupleIndex];
            }                                         
            public double getProbability() {
                assert readTupleIndex < writeTupleIndex;
                return probabilitys[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getNumber(), getProbability());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexWord(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processWord(getWord());
                    assert getWordEndIndex() <= endIndex;
                    copyTuples(getWordEndIndex(), output);
                    incrementWord();
                }
            }  
            public void copyUntilWord(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getWord(), other.getWord());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processWord(getWord());
                                      
                        copyTuples(getWordEndIndex(), output);
                    } else {
                        output.processWord(getWord());
                        copyTuples(getWordEndIndex(), output);
                    }
                    incrementWord();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilWord(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<NumberWordProbability>, ShreddedSource {
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
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

            public NumberWordProbability read() throws IOException {
                if (uninitialized)
                    initialize();

                NumberWordProbability result = null;

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
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<NumberWordProbability>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            NumberWordProbability last = new NumberWordProbability();         
            long updateWordCount = -1;
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
                    result = + CmpUtil.compare(buffer.getWord(), otherBuffer.getWord());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final NumberWordProbability read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                NumberWordProbability result = new NumberWordProbability();
                
                result.word = buffer.getWord();
                result.number = buffer.getNumber();
                result.probability = buffer.getProbability();
                
                buffer.incrementTuple();
                buffer.autoIncrementWord();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateWordCount - tupleCount > 0) {
                            buffer.words.add(last.word);
                            buffer.wordTupleIdx.add((int) (updateWordCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateWord();
                        buffer.processTuple(input.readLong(), input.readDouble());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateWord() throws IOException {
                if (updateWordCount > tupleCount)
                    return;
                     
                last.word = input.readBytes();
                updateWordCount = tupleCount + input.readInt();
                                      
                buffer.processWord(last.word);
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
                } else if (processor instanceof NumberWordProbability.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((NumberWordProbability.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<NumberWordProbability>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<NumberWordProbability> getOutputClass() {
                return NumberWordProbability.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            NumberWordProbability last = new NumberWordProbability();
            boolean wordProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processWord(byte[] word) throws IOException {  
                if (wordProcess || CmpUtil.compare(word, last.word) != 0) {
                    last.word = word;
                    processor.processWord(word);
                    wordProcess = false;
                }
            }  
            
            public void resetWord() {
                 wordProcess = true;
            }                                                
                               
            public void processTuple(long number, double probability) throws IOException {
                processor.processTuple(number, probability);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            NumberWordProbability last = new NumberWordProbability();
            public org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor;                               
            
            public TupleUnshredder(NumberWordProbability.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<NumberWordProbability> processor) {
                this.processor = processor;
            }
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void processWord(byte[] word) throws IOException {
                last.word = word;
            }   
                
            
            public void processTuple(long number, double probability) throws IOException {
                last.number = number;
                last.probability = probability;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            NumberWordProbability last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public NumberWordProbability clone(NumberWordProbability object) {
                NumberWordProbability result = new NumberWordProbability();
                if (object == null) return result;
                result.number = object.number; 
                result.word = object.word; 
                result.probability = object.probability; 
                return result;
            }                 
            
            public void process(NumberWordProbability object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.word, object.word) != 0 || processAll) { processor.processWord(object.word); processAll = true; }
                processor.processTuple(object.number, object.probability);                                         
                last = object;
            }
                          
            public Class<NumberWordProbability> getInputClass() {
                return NumberWordProbability.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    