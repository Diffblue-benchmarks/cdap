/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.templates.etl.realtime.sinks;

import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.DataWriter;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic NoOp Sink.
 *
 * @param <T> any object
 */
public class NoOpSink<T> extends RealtimeSink<T> {
  private static final Logger LOG = LoggerFactory.getLogger(NoOpSink.class);

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(NoOpSink.class.getSimpleName());
  }

  @Override
  public int write(Iterable<T> objects, DataWriter dataWriter) throws Exception {
    for (T object : objects) {
      LOG.error("Received : {}", object);
    }
    return 0;
  }
}
