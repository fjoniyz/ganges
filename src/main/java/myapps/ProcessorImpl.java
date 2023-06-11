package myapps;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProcessorImpl implements Processor<String, String, Void, Void> {
  private KeyValueStore<String, String> stateStore;
  private ProcessorContext context;
  private String storeName;

  public ProcessorImpl(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(final ProcessorContext<Void, Void> context) {
    this.context = context;
    this.stateStore = context.getStateStore(storeName);
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'close'");
  }

  @Override
  public void process(Record<String, String> record) {
    KeyValueIterator<String, String> iterator = this.stateStore.all();
    while (iterator.hasNext()){
      System.out.println("State store: " + iterator.next().value);
    }
  }
}
