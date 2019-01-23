/**
 * Licensed to The Apereo Foundation under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 *
 * The Apereo Foundation licenses this file to you under the Educational
 * Community License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License
 * at:
 *
 *   http://opensource.org/licenses/ecl2.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.opencastproject.assetmanager.api.query;

import org.opencastproject.assetmanager.api.DeleteSnapshotHandler;

public interface ADeleteQuery {
  ADeleteQuery where(Predicate predicate);

  /**
   * Name the query for debugging purposes. The name will be printed in the logs.
   */
  ADeleteQuery name(String queryName);

  /**
   * When this is called with true, the AssetManager will assume that the whole media package will be removed. This is
   * faster, but then it is your responsibility to remove all properties associated with the media package since no
   * orphan removal will take place.
   */
  ADeleteQuery willRemoveWholeMediaPackage(boolean willRemoveWholeMediaPackage);

  /**
   * Delete the selected items.
   *
   * @param deleteSnapshotHandler callback to learn about deleted snapshots and episodes
   * @return the number of affected items
   */
  long run(DeleteSnapshotHandler deleteSnapshotHandler);

  /** Call {@link #run()} with a deletion handler that does nothing. */
  default long run() {
    return run(DeleteSnapshotHandler.NOP_DELETE_SNAPSHOT_HANDLER);
  };
}
