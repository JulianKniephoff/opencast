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

package org.opencastproject.event.handler;

import org.opencastproject.assetmanager.api.AssetManager;
import org.opencastproject.mediapackage.MediaPackageElementFlavor;
import org.opencastproject.mediapackage.Publication;
import org.opencastproject.message.broker.api.assetmanager.AssetManagerItem;
import org.opencastproject.message.broker.api.update.AssetManagerUpdateHandler;
import org.opencastproject.publication.api.EngagePublicationService;
import org.opencastproject.publication.api.PublicationException;
import org.opencastproject.security.api.SecurityService;
import org.opencastproject.workflow.handler.distribution.EngagePublicationChannel;

import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

// TODO Note: This does **not** work; it's just a PoC.
//   I put it into it's own handler so I didn't have to worry about the existing code too much.
@Component(
    immediate = true,
    service = {
        AssetManagerUpdateHandler.class
    },
    property = {
        "service.description=Republish Metadata Event Handler"
    }
)
public class RepublishMetadataEventHandler implements AssetManagerUpdateHandler {

  private static final Logger logger = LoggerFactory.getLogger(RepublishMetadataEventHandler.class);

  private EngagePublicationService engagePublicationService;
  private EngagePublicationService downloadEngagePublicationService;
  private EngagePublicationService awsS3EngagePublicationService;
  private SecurityService securityService;
  private AssetManager assetManager;

  @Reference(target = "(distribution.channel=download)")
  public void setDownloadEngagePublicationService(EngagePublicationService downloadEngagePublicationService) {
    this.downloadEngagePublicationService = downloadEngagePublicationService;
  }

  @Reference(target = "(distribution.channel=aws.s3)")
  public void setAwsS3EngagePublicationService(EngagePublicationService awsS3EngagePublicationService) {
    this.awsS3EngagePublicationService = awsS3EngagePublicationService;
  }

  @Reference
  public void setSecurityService(SecurityService securityService) {
    this.securityService = securityService;
  }

  @Reference
  public void setAssetManager(AssetManager assetManager) {
    this.assetManager = assetManager;
  }

  @Activate
  public void activate(ComponentContext cc) {
    logger.info("Activating {}", RepublishMetadataEventHandler.class.getName());

    // TODO This needs to be assigned based on a config value
    engagePublicationService = downloadEngagePublicationService;
  }

  @Override
  public void execute(AssetManagerItem messageItem) {
    if (!(messageItem instanceof AssetManagerItem.TakeSnapshot)) {
      // We don't want to handle anything but TakeSnapshot messages.
      return;
    }
    var snapshotItem = (AssetManagerItem.TakeSnapshot) messageItem;
    var mediaPackage = snapshotItem.getMediapackage();

    // TODO Potentially figure out whether this snapshot even merits republication,
    //   and potentially return early.

    var published = false;

    // If we have an Engage publication ...
    if (Arrays.stream(mediaPackage.getPublications())
        .map(Publication::getChannel)
        .anyMatch(EngagePublicationChannel.CHANNEL_ID::equals)) {
      try {
        // TODO I'm calling this as a PoC with what are basically the parameters that are passed–potentially implicitly–
        //   by the `republish-metadata`-workflow.
        //   See also https://docs.opencast.org/develop/admin/#workflowoperationhandlers/publish-engage-woh
        //   for the defaults.
        //   This might be fine, actually, or we might want to make some of these things configurable,
        //   like in the original PR.
        var publishedMpOpt = engagePublicationService.publish(mediaPackage,
            new String[] { "dublincore/*", "security/*" }, new String[0], new String[0], new String[0],
            EngagePublicationService.PUBLISH_STRATEGY_MERGE, false, null, new String[0],
            null, new String[0], List.of(MediaPackageElementFlavor.parseFlavor("dublincore/*"),
                MediaPackageElementFlavor.parseFlavor("security/*")), null, securityService.getOrganization(), true);

        // TODO Potentially check whether the publication even changed;
        //   The WOH-Code doesn't always return SKIP/empty when that's not the case ...
        //   Of course one could change that as well. ¯\_(ツ)_/¯
        published = published || publishedMpOpt.isPresent();
      } catch (PublicationException e) {
        // TODO What do we do if a publication fails?
      }
    }

    // TODO Update other channels by calling the appropriate services

    if (published) {
      // TODO Figure out exactly when you need to do this
      assetManager.takeSnapshot(mediaPackage);
    }
  }
}
