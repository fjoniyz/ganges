package myapps;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;

public class ProcessorContextImpl implements Processor<String, String, Void, Void> {
  private KeyValueStore<String, String> stateStore;
  private String storeName;

  public ProcessorContextImpl(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public void init(final ProcessorContext<Void, Void> context) {
    this.stateStore = context.getStateStore(storeName);
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'close'");
  }

  @Override
  public void process(Record<String, String> record) {
    this.stateStore.put(record.key(), record.value());

    printValues();
  }
  // Just to check whether the values are saved in state store or not
  public void printValues() {
    KeyValueIterator<String, String> iterator = this.stateStore.all();
    while (iterator.hasNext()) {
      KeyValue<String, String> entry = iterator.next();
      System.out.println("Entry: " + entry.value);
    }
  }
}
