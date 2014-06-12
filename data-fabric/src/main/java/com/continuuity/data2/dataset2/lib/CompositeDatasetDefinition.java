package com.continuuity.data2.dataset2.lib;

import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetProperties;
import com.continuuity.internal.data.dataset.DatasetSpecification;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handy implementation of {@link DatasetDefinition} that implements basic methods by delegating logic execution to
 * underlying dataset definitions.
 *
 * @param <D> defines data operations that can be performed on this dataset instance
 */
public abstract class CompositeDatasetDefinition<D extends Dataset>
  extends AbstractDatasetDefinition<D, DatasetAdmin> {

  private final Map<String, ? extends DatasetDefinition> delegates;

  /**
   * Constructor that takes an info about underlying datasets
   * @param name this dataset type name
   * @param delegates map of [dataset instance name] -> [dataset definition] to use for this instance name
   */
  protected CompositeDatasetDefinition(String name, Map<String, ? extends DatasetDefinition> delegates) {
    super(name);
    this.delegates = delegates;
  }

  /**
   * Gets dataset for underlying dataset instance of given name
   * @param name dataset instance name
   * @param type expected type of the dataset
   * @param spec parent dataset instance name
   * @param <T> expected type of the dataset
   * @return dataset to perform data operations
   * @throws IOException
   */
  protected final <T extends Dataset> T getDataset(String name, Class<T> type, DatasetSpecification spec)
    throws IOException {

    return (T) delegates.get(name).getDataset(spec.getSpecification(name));
  }

  protected final <T extends Dataset> T getDataset(String name, DatasetSpecification spec)
    throws IOException {

    // NOTE: by default we propagate properties to the embedded datasets
    return (T) delegates.get(name).getDataset(spec.getSpecification(name));
  }

  @Override
  public final DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    List<DatasetSpecification> specs = Lists.newArrayList();
    for (Map.Entry<String, ? extends DatasetDefinition> impl : this.delegates.entrySet()) {
      specs.add(impl.getValue().configure(impl.getKey(), properties));
    }

    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(specs)
      .build();
  }

  @Override
  public final DatasetAdmin getAdmin(DatasetSpecification spec) throws IOException {
    List<DatasetAdmin> admins = Lists.newArrayList();
    for (Map.Entry<String, ? extends DatasetDefinition> impl : this.delegates.entrySet()) {
      admins.add(impl.getValue().getAdmin(spec.getSpecification(impl.getKey())));
    }

    return new CompositeDatasetAdmin(admins);
  }
}

