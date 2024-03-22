/*
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

package org.opencastproject.workflow.handler.distribution;

import static org.opencastproject.publication.api.EngagePublicationService.PUBLISH_STRATEGY_DEFAULT;
import static org.opencastproject.util.data.Option.option;
import static org.opencastproject.util.data.functions.Strings.toBool;
import static org.opencastproject.util.data.functions.Strings.trimToNone;

import org.opencastproject.job.api.JobContext;
import org.opencastproject.mediapackage.MediaPackage;
import org.opencastproject.mediapackage.MediaPackageElementFlavor;
import org.opencastproject.publication.api.EngagePublicationService;
import org.opencastproject.publication.api.PublicationException;
import org.opencastproject.security.api.OrganizationDirectoryService;
import org.opencastproject.util.NotFoundException;
import org.opencastproject.workflow.api.AbstractWorkflowOperationHandler;
import org.opencastproject.workflow.api.WorkflowInstance;
import org.opencastproject.workflow.api.WorkflowOperationException;
import org.opencastproject.workflow.api.WorkflowOperationHandler;
import org.opencastproject.workflow.api.WorkflowOperationInstance;
import org.opencastproject.workflow.api.WorkflowOperationResult;
import org.opencastproject.workflow.api.WorkflowOperationResult.Action;

import org.apache.commons.lang3.StringUtils;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The workflow definition for handling "engage publication" operations
 */

@Component(
    immediate = true,
    service = WorkflowOperationHandler.class,
    property = {
        "service.description=Engage Publication Workflow Handler",
        "workflow.operation=publish-engage"
    }
)
public class PublishEngageWorkflowOperationHandler extends AbstractWorkflowOperationHandler {

  /** The logging facility */
  private static final Logger logger = LoggerFactory.getLogger(PublishEngageWorkflowOperationHandler.class);

  /** Workflow configuration option keys */
  static final String DOWNLOAD_SOURCE_FLAVORS = "download-source-flavors";
  static final String DOWNLOAD_TARGET_SUBFLAVOR = "download-target-subflavor";
  static final String DOWNLOAD_SOURCE_TAGS = "download-source-tags";
  static final String DOWNLOAD_TARGET_TAGS = "download-target-tags";
  static final String STREAMING_SOURCE_TAGS = "streaming-source-tags";
  static final String STREAMING_TARGET_TAGS = "streaming-target-tags";
  static final String STREAMING_SOURCE_FLAVORS = "streaming-source-flavors";
  static final String STREAMING_TARGET_SUBFLAVOR = "streaming-target-subflavor";
  static final String CHECK_AVAILABILITY = "check-availability";
  static final String STRATEGY = "strategy";
  static final String MERGE_FORCE_FLAVORS = "merge-force-flavors";
  static final String ADD_FORCE_FLAVORS = "add-force-flavors";

  private static final String MERGE_FORCE_FLAVORS_DEFAULT = "dublincore/*,security/*";
  private static final String ADD_FORCE_FLAVORS_DEFAULT = "";

  private EngagePublicationService publicationService;

  private OrganizationDirectoryService organizationDirectoryService;

  @Reference
  public void setOrganizationDirectoryService(OrganizationDirectoryService organizationDirectoryService) {
    this.organizationDirectoryService = organizationDirectoryService;
  }

  @Reference(target = "(distribution.channel=download)")
  public void setPublicationService(EngagePublicationService publicationService) {
    this.publicationService = publicationService;
  }

  @Override
  public WorkflowOperationResult start(final WorkflowInstance workflowInstance, JobContext context)
          throws WorkflowOperationException {
    logger.debug("Running engage publication workflow operation");

    MediaPackage mediaPackage = workflowInstance.getMediaPackage();
    WorkflowOperationInstance op = workflowInstance.getCurrentOperation();

    // Check which tags have been configured
    String downloadSourceTags = StringUtils.trimToEmpty(op.getConfiguration(DOWNLOAD_SOURCE_TAGS));
    String downloadTargetTags = StringUtils.trimToEmpty(op.getConfiguration(DOWNLOAD_TARGET_TAGS));
    String downloadSourceFlavors = StringUtils.trimToEmpty(op.getConfiguration(DOWNLOAD_SOURCE_FLAVORS));
    String downloadTargetSubflavor = StringUtils.trimToNull(op.getConfiguration(DOWNLOAD_TARGET_SUBFLAVOR));
    String streamingSourceTags = StringUtils.trimToEmpty(op.getConfiguration(STREAMING_SOURCE_TAGS));
    String streamingTargetTags = StringUtils.trimToEmpty(op.getConfiguration(STREAMING_TARGET_TAGS));
    String streamingSourceFlavors = StringUtils.trimToEmpty(op.getConfiguration(STREAMING_SOURCE_FLAVORS));
    String streamingTargetSubflavor = StringUtils.trimToNull(op.getConfiguration(STREAMING_TARGET_SUBFLAVOR));
    String republishStrategy = StringUtils.trimToEmpty(
        StringUtils.defaultString(op.getConfiguration(STRATEGY), PUBLISH_STRATEGY_DEFAULT));
    String mergeForceFlavorsStr = StringUtils.trimToEmpty(
        StringUtils.defaultString(op.getConfiguration(MERGE_FORCE_FLAVORS), MERGE_FORCE_FLAVORS_DEFAULT));
    String addForceFlavorsStr = StringUtils.trimToEmpty(
        StringUtils.defaultString(op.getConfiguration(ADD_FORCE_FLAVORS), ADD_FORCE_FLAVORS_DEFAULT));

    boolean checkAvailability = option(op.getConfiguration(CHECK_AVAILABILITY)).bind(trimToNone).map(toBool)
        .getOrElse(true);

    String[] sourceDownloadTags = StringUtils.split(downloadSourceTags, ",");
    String[] targetDownloadTags = StringUtils.split(downloadTargetTags, ",");
    String[] sourceDownloadFlavors = StringUtils.split(downloadSourceFlavors, ",");
    String[] sourceStreamingTags = StringUtils.split(streamingSourceTags, ",");
    String[] targetStreamingTags = StringUtils.split(streamingTargetTags, ",");
    String[] sourceStreamingFlavors = StringUtils.split(streamingSourceFlavors, ",");

    if (sourceDownloadTags.length == 0 && sourceDownloadFlavors.length == 0 && sourceStreamingTags.length == 0
        && sourceStreamingFlavors.length == 0) {
      logger.warn("No tags or flavors have been specified, so nothing will be published to the "
          + "engage publication channel");
      return createResult(mediaPackage, Action.CONTINUE);
    }

    // Parse forced flavors
    List<MediaPackageElementFlavor> mergeForceFlavors = Arrays.stream(StringUtils.split(mergeForceFlavorsStr, ", "))
        .map(MediaPackageElementFlavor::parseFlavor).collect(Collectors.toList());
    List<MediaPackageElementFlavor> addForceFlavors = Arrays.stream(StringUtils.split(addForceFlavorsStr, ", "))
        .map(MediaPackageElementFlavor::parseFlavor).collect(Collectors.toList());

    // Parse the download target flavor
    MediaPackageElementFlavor downloadSubflavor = null;
    if (downloadTargetSubflavor != null) {
      try {
        downloadSubflavor = MediaPackageElementFlavor.parseFlavor(downloadTargetSubflavor);
      } catch (IllegalArgumentException e) {
        throw new WorkflowOperationException(e);
      }
    }

    // Parse the streaming target flavor
    MediaPackageElementFlavor streamingSubflavor = null;
    if (streamingTargetSubflavor != null) {
      try {
        streamingSubflavor = MediaPackageElementFlavor.parseFlavor(streamingTargetSubflavor);
      } catch (IllegalArgumentException e) {
        throw new WorkflowOperationException(e);
      }
    }

    try {
      return publicationService.publish(mediaPackage, sourceDownloadFlavors, sourceDownloadTags, sourceStreamingFlavors,
              sourceStreamingTags, republishStrategy, checkAvailability, downloadSubflavor, targetDownloadTags,
              streamingSubflavor, targetStreamingTags, mergeForceFlavors, addForceFlavors,
              organizationDirectoryService.getOrganization(workflowInstance.getOrganizationId()), false)
          .map(mp -> createResult(mp, Action.CONTINUE))
          .orElseGet(() -> createResult(mediaPackage, Action.SKIP));
    } catch (NotFoundException e) {
      throw new WorkflowOperationException(e);
    } catch (PublicationException e) {
      throw new WorkflowOperationException(e.getCause());
    }
  }
}
