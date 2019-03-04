/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.logbuffer.cleaner;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.logbuffer.LogBufferFileOffset;
import co.cask.cdap.logging.meta.CheckpointManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Log Buffer cleaner service. It periodically cleans up log buffer files for which logs are already persisted.
 */
public class LogBufferCleanerService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(LogBufferCleanerService.class);
  private static final String SERVICE_NAME = "log.buffer.cleaner";

  private final List<CheckpointManager<LogBufferFileOffset>> checkpointManagers;
  private final int batchSize;
  private final File baseLogDir;
  private final long delayMillis;
  private ScheduledExecutorService executor;

  public LogBufferCleanerService(CConfiguration cConf,
                                 List<CheckpointManager<LogBufferFileOffset>> checkpointManagers) {
    this(checkpointManagers, cConf.get(Constants.LogBuffer.LOG_BUFFER_BASE_DIR),
         cConf.getLong(Constants.LogBuffer.LOG_BUFFER_CLEANER_DELAY_MILLIS),
         cConf.getInt(Constants.LogBuffer.LOG_BUFFER_CLEANER_BATCH_SIZE));
  }

  @VisibleForTesting
  LogBufferCleanerService(List<CheckpointManager<LogBufferFileOffset>> checkpointManagers, String baseLogDir,
                          long delayMillis, int batchSize) {
    this.checkpointManagers = checkpointManagers;
    this.batchSize = batchSize;
    this.baseLogDir = new File(baseLogDir);
    this.delayMillis = delayMillis;
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory(SERVICE_NAME));
    return executor;
  }

  @Override
  protected void runOneIteration() {
    if (!baseLogDir.exists()) {
      return;
    }

    try {
      // From all the pipelines, get the smallest file id for which logs have been persisted. So the cleaner can delete
      // files with file id smaller than smallestFileId.
      long smallestFileId = getSmallestFileId(checkpointManagers);

      // scan files under baseLogDir and delete file which has fileId smaller than smallestFileId
      scanAndDelete(baseLogDir, smallestFileId);
    } catch (IOException e) {
      // if any exception occurs, just log it and retry in the next iteration.
      LOG.warn("Exception while cleaning up log buffer.", e);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return new CustomScheduler() {
      @Override
      protected Schedule getNextSchedule() {
        return new Schedule(delayMillis, TimeUnit.MILLISECONDS);
      }
    };
  }

  @Override
  protected void shutDown() {
    if (executor != null) {
      executor.shutdown();
    }
  }

  /**
   * Scans base log directory and deletes file which has fileId smaller than smallestFileId.
   */
  private void scanAndDelete(File baseLogDir, long smallestFileId) {
    // if the offsets are not persisted yet, then nothing needs to be done.
    if (smallestFileId == -1) {
      return;
    }

    File[] files = baseLogDir.listFiles();
    if (files != null) {
      int deleted = 0;
      int index = 0;
      while (deleted < batchSize) {
        File file = files[index++];
        if (getFileId(file.getName()) < smallestFileId) {
          // if the file is not deleted, in the next scan it should get deleted.
          file.delete();
          deleted++;
        }
      }
    }
  }

  /**
   * Get smallest fileId for which logs have been persisted.
   */
  private long getSmallestFileId(List<CheckpointManager<LogBufferFileOffset>> checkpointManagers) throws IOException {
    // there will be atleast one log pipeline
    LogBufferFileOffset minOffset = checkpointManagers.get(0).getCheckpoint(0).getOffset();

    for (int i = 1; i < checkpointManagers.size(); i++) {
      LogBufferFileOffset offset = checkpointManagers.get(i).getCheckpoint(0).getOffset();
      // keep track of minimum offset
      minOffset = minOffset.compareTo(offset) > 0 ? offset : minOffset;
    }

    return minOffset.getFileId();
  }

  /**
   * Given the file name, returns the file id.
   */
  private long getFileId(String fileName) {
    String[] splitted = fileName.split("\\.");
    return Long.parseLong(splitted[0]);
  }
}
