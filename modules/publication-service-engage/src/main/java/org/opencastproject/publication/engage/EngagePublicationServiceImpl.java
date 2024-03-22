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
package org.opencastproject.publication.engage;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.opencastproject.systems.OpencastConstants.SERVER_URL_PROPERTY;
import static org.opencastproject.util.JobUtil.waitForJobs;
import static org.opencastproject.workflow.handler.distribution.EngagePublicationChannel.CHANNEL_ID;

import org.opencastproject.distribution.api.DistributionException;
import org.opencastproject.distribution.api.DownloadDistributionService;
import org.opencastproject.distribution.api.StreamingDistributionService;
import org.opencastproject.job.api.Job;
import org.opencastproject.mediapackage.Attachment;
import org.opencastproject.mediapackage.Catalog;
import org.opencastproject.mediapackage.MediaPackage;
import org.opencastproject.mediapackage.MediaPackageElement;
import org.opencastproject.mediapackage.MediaPackageElementFlavor;
import org.opencastproject.mediapackage.MediaPackageElementParser;
import org.opencastproject.mediapackage.MediaPackageElements;
import org.opencastproject.mediapackage.MediaPackageException;
import org.opencastproject.mediapackage.MediaPackageReference;
import org.opencastproject.mediapackage.MediaPackageReferenceImpl;
import org.opencastproject.mediapackage.Publication;
import org.opencastproject.mediapackage.PublicationImpl;
import org.opencastproject.mediapackage.Track;
import org.opencastproject.mediapackage.selector.SimpleElementSelector;
import org.opencastproject.mediapackage.track.TrackImpl;
import org.opencastproject.metadata.dublincore.DublinCore;
import org.opencastproject.metadata.dublincore.DublinCoreValue;
import org.opencastproject.metadata.dublincore.DublinCoreXmlFormat;
import org.opencastproject.publication.api.EngagePublicationService;
import org.opencastproject.publication.api.PublicationException;
import org.opencastproject.search.api.SearchException;
import org.opencastproject.search.api.SearchQuery;
import org.opencastproject.search.api.SearchResult;
import org.opencastproject.search.api.SearchService;
import org.opencastproject.security.api.Organization;
import org.opencastproject.security.api.UnauthorizedException;
import org.opencastproject.serviceregistry.api.ServiceRegistry;
import org.opencastproject.util.MimeTypes;
import org.opencastproject.util.NotFoundException;
import org.opencastproject.util.UrlSupport;
import org.opencastproject.workflow.api.WorkflowOperationException;
import org.opencastproject.workspace.api.Workspace;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIUtils;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Component(
    immediate = true,
    service = EngagePublicationService.class,
    property = {
        "service.description=Publication Service (Engage)",
        "distribution.channel=download"
    }
)
public class EngagePublicationServiceImpl implements EngagePublicationService {
  @Override
  public Optional<MediaPackage> publish(MediaPackage mediaPackage, String[] sourceDownloadFlavors,
      String[] sourceDownloadTags, String[] sourceStreamingFlavors, String[] sourceStreamingTags,
      String republishStrategy, boolean checkAvailability, MediaPackageElementFlavor downloadSubflavor,
      String[] targetDownloadTags, MediaPackageElementFlavor streamingSubflavor, String[] targetStreamingTags,
      List<MediaPackageElementFlavor> mergeForceFlavors, List<MediaPackageElementFlavor> addForceFlavors,
      Organization organization, boolean sync) throws PublicationException {

    // First check if mp exists in the search index and strategy is merge
    // to avoid leaving distributed elements around.
    MediaPackage distributedMp = getDistributedMediapackage(mediaPackage.getIdentifier().toString());
    if (PUBLISH_STRATEGY_MERGE.equals(republishStrategy) && distributedMp == null) {
      logger.info("Skipping republish for {} since it is not currently published",
          mediaPackage.getIdentifier().toString());
      return Optional.empty();
    }

    // Configure the download element selector
    SimpleElementSelector downloadElementSelector = new SimpleElementSelector();
    for (String flavor : sourceDownloadFlavors) {
      downloadElementSelector.addFlavor(MediaPackageElementFlavor.parseFlavor(flavor));
    }
    for (String tag : sourceDownloadTags) {
      downloadElementSelector.addTag(tag);
    }

    // Configure the streaming element selector
    SimpleElementSelector streamingElementSelector = new SimpleElementSelector();
    for (String flavor : sourceStreamingFlavors) {
      streamingElementSelector.addFlavor(MediaPackageElementFlavor.parseFlavor(flavor));
    }
    for (String tag : sourceStreamingTags) {
      streamingElementSelector.addTag(tag);
    }

    // Select the appropriate elements for download and streaming
    Collection<MediaPackageElement> downloadElements = downloadElementSelector.select(mediaPackage, false);
    Collection<MediaPackageElement> streamingElements = streamingElementSelector.select(mediaPackage, false);

    try {
      Set<String> downloadElementIds = new HashSet<>();
      Set<String> streamingElementIds = new HashSet<>();

      // Look for elements matching the tag
      for (MediaPackageElement elem : downloadElements) {
        downloadElementIds.add(elem.getIdentifier());
      }
      for (MediaPackageElement elem : streamingElements) {
        streamingElementIds.add(elem.getIdentifier());
      }

      removePublicationElement(mediaPackage);
      if (republishStrategy.equals(PUBLISH_STRATEGY_DEFAULT) && distributedMp != null) {
        retractFromEngage(distributedMp, sync);
      }

      List<Job> jobs = new ArrayList<>(2);
      List<MediaPackageElement> distributedElements = new ArrayList<>();
      // distribute Elements
      try {
        if (!downloadElementIds.isEmpty()) {
          if (sync) {
            distributedElements.addAll(
                downloadDistributionService.distributeSync(CHANNEL_ID, mediaPackage, downloadElementIds,
                    checkAvailability));
          } else {
            Job job = downloadDistributionService.distribute(CHANNEL_ID, mediaPackage, downloadElementIds,
                checkAvailability);
            if (job != null) {
              jobs.add(job);
            }
          }

          if (streamingDistributionService != null && streamingDistributionService.publishToStreaming()) {
            if (sync) {
              distributedElements.addAll(
                  streamingDistributionService.distributeSync(CHANNEL_ID, mediaPackage, streamingElementIds));
            } else {
              Job job = streamingDistributionService.distribute(CHANNEL_ID, mediaPackage, streamingElementIds);
              if (job != null) {
                jobs.add(job);
              }
            }
          }
        }
      } catch (DistributionException e) {
        throw new WorkflowOperationException(e);
      }

      if (jobs.isEmpty()) {
        logger.info("No mediapackage element was found for distribution to engage");
        return Optional.of(mediaPackage);
      }

      // Wait until all distribution jobs have returned
      if (!sync) {
        if (!waitForJobs(null, serviceRegistry, jobs.toArray(new Job[jobs.size()])).isSuccess()) {
          throw new WorkflowOperationException("One of the distribution jobs did not complete successfully");
        }

        for (Job job : jobs) {
          Job resolvedJob = serviceRegistry.getJob(job.getId());

          // If there is no payload, then the item has not been distributed.
          if (resolvedJob.getPayload() == null) {
            continue;
          }

          distributedElements.addAll(MediaPackageElementParser.getArrayFromXml(resolvedJob.getPayload()));
        }
      }

      logger.debug("Distribute of mediapackage {} completed", mediaPackage);

      String engageUrlString = null;
      try {
        MediaPackage mediaPackageForSearch = getMediaPackageForSearchIndex(mediaPackage, distributedElements,
            downloadSubflavor, targetDownloadTags, downloadElementIds, streamingSubflavor, streamingElementIds,
            targetStreamingTags);

        // MH-10216, check if only merging into existing mediapackage
        removePublicationElement(mediaPackage);
        if (republishStrategy.equals(PUBLISH_STRATEGY_MERGE)) {
          mediaPackageForSearch = mergePackages(mediaPackageForSearch, distributedMp, mergeForceFlavors,
              addForceFlavors);
        }

        if (StringUtils.isBlank(mediaPackageForSearch.getTitle())) {
          var dcUri = Arrays.stream(mediaPackageForSearch.getCatalogs(MediaPackageElements.EPISODE)).findFirst()
              .map(MediaPackageElement::getURI);
          if (dcUri.isPresent()) {
            try (var in = workspace.read(dcUri.get())) {
              DublinCoreXmlFormat.read(in).get(DublinCore.PROPERTY_TITLE).stream().findFirst()
                  .map(DublinCoreValue::getValue).ifPresent(mediaPackageForSearch::setTitle);
            }
          }
        }

        // Check that the media package meets the criteria for publication
        if (isBlank(mediaPackageForSearch.getTitle())) {
          throw new WorkflowOperationException("Media package does not meet publication criteria: Missing title");
        }
        if (!mediaPackageForSearch.hasTracks()) {
          throw new WorkflowOperationException("Media package does not meet publication criteria: No tracks selected");
        }

        // Prepare published elements to be added
        MediaPackageElement[] mediaPackageElements = mediaPackageForSearch.getElements();

        logger.info("Publishing media package {} to search index", mediaPackageForSearch);

        URL engageBaseUrl;
        engageUrlString = StringUtils.trimToNull(organization.getProperties().get(ENGAGE_URL_PROPERTY));
        if (engageUrlString != null) {
          engageBaseUrl = new URL(engageUrlString);
        } else {
          engageBaseUrl = serverUrl;
          logger.info("Using 'server.url' as a fallback for the non-existing organization level key '{}' "
              + "for the publication url", ENGAGE_URL_PROPERTY);
        }

        // create the publication URI (used by Admin UI for event details link)
        URI engageUri = URIUtils.resolve(engageBaseUrl.toURI(), PLAYER_PATH + mediaPackage.getIdentifier().toString());

        // Create new distribution element
        Publication publicationElement = PublicationImpl.publication(UUID.randomUUID().toString(), CHANNEL_ID,
            engageUri, MimeTypes.parseMimeType("text/html"));

        // Add published elements
        for (MediaPackageElement element : mediaPackageElements) {
          element.setIdentifier(null);
          PublicationImpl.addElementToPublication(publicationElement, element);
        }

        mediaPackage.add(publicationElement);

        // create publication URI for streaming
        if (streamingDistributionService != null && streamingDistributionService.publishToStreaming()
            && !publishedStreamingFormats.isEmpty()) {
          for (Track track : mediaPackageForSearch.getTracks()) {
            String mimeType = track.getMimeType().toString();
            if (isStreamingFormat(track) && (publishedStreamingFormats.contains(mimeType)
                || publishedStreamingFormats.contains("*"))) {
              publicationElement.addTrack(track);
            }
          }
          for (Attachment attachment : mediaPackageForSearch.getAttachments()) {
            publicationElement.addAttachment(attachment);
          }
          for (Catalog catalog : mediaPackageForSearch.getCatalogs()) {
            publicationElement.addCatalog(catalog);
          }
        }

        // Adding media package to the search index
        try {
          if (sync) {
            searchService.addSynchronously(mediaPackageForSearch);
          } else {
            Job publishJob = searchService.add(mediaPackageForSearch);
            if (!waitForJobs(null, serviceRegistry, publishJob).isSuccess()) {
              throw new WorkflowOperationException(
                  "Mediapackage " + mediaPackageForSearch.getIdentifier() + " could not be published");
            }
          }
        } catch (SearchException e) {
          throw new WorkflowOperationException("Error publishing media package", e);
        } catch (MediaPackageException e) {
          throw new WorkflowOperationException("Error parsing media package", e);
        }

        logger.debug("Publishing of mediapackage {} completed", mediaPackage);
        return Optional.of(mediaPackage);
      } catch (MalformedURLException e) {
        logger.error("{} is malformed: {}", ENGAGE_URL_PROPERTY, engageUrlString);
        throw new PublicationException(e);
      } catch (Throwable t) {
        if (t instanceof PublicationException) {
          throw (PublicationException) t;
        } else {
          throw new PublicationException(t);
        }
      }
    } catch (Exception e) {
      if (e instanceof PublicationException) {
        throw (PublicationException) e;
      } else {
        throw new PublicationException(e);
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(EngagePublicationServiceImpl.class);
  private SearchService searchService;
  private DownloadDistributionService downloadDistributionService;
  private StreamingDistributionService streamingDistributionService;
  private Workspace workspace;

  @Reference
  public void setSearchService(SearchService searchService) {
    this.searchService = searchService;
  }

  @Reference(target = "(distribution.channel=download)")
  public void setDownloadDistributionService(DownloadDistributionService downloadDistributionService) {
    this.downloadDistributionService = downloadDistributionService;
  }

  @Reference(target = "(distribution.channel=streaming)")
  public void setStreamingDistributionService(StreamingDistributionService streamingDistributionService) {
    this.streamingDistributionService = streamingDistributionService;
  }

  @Reference
  public void setWorkspace(Workspace workspace) {
    this.workspace = workspace;
  }

  private MediaPackage getDistributedMediapackage(String mediaPackageId) {
    SearchQuery query = new SearchQuery().withId(mediaPackageId);
    query.includeEpisodes(true);
    query.includeSeries(false);
    SearchResult result = searchService.getByQuery(query);
    if (result.size() == 0) {
      logger.info("The search service doesn't know mediapackage {}.", mediaPackageId);
      return null;
    } else if (result.size() > 1) {
      logger.warn("More than one mediapackage with id {} returned from search service", mediaPackageId);
      throw new IllegalStateException("More than one mediapackage with id " + mediaPackageId + " found");
    } else {
      // else, merge the new with the existing (new elements will overwrite existing elements)
      return result.getItems()[0].getMediaPackage();
    }
  }

  private void removePublicationElement(MediaPackage mediaPackage) {
    for (Publication publicationElement : mediaPackage.getPublications()) {
      if (CHANNEL_ID.equals(publicationElement.getChannel())) {
        mediaPackage.remove(publicationElement);
      }
    }
  }

  private void retractFromEngage(MediaPackage distributedMediaPackage, boolean sync) throws PublicationException {
    List<Job> jobs = new ArrayList<>(3);
    Set<String> elementIds = Arrays.stream(distributedMediaPackage.getElements())
        .map(MediaPackageElement::getIdentifier)
        .collect(Collectors.toSet());
    try {
      if (!elementIds.isEmpty()) {
        if (sync) {
          downloadDistributionService.retractSync(CHANNEL_ID, distributedMediaPackage, elementIds);
        } else {
          Job retractDownloadDistributionJob = downloadDistributionService.retract(CHANNEL_ID, distributedMediaPackage,
              elementIds);
          if (retractDownloadDistributionJob != null) {
            jobs.add(retractDownloadDistributionJob);
          }
        }

        if (streamingDistributionService != null && streamingDistributionService.publishToStreaming()) {
          if (sync) {
            streamingDistributionService.retractSync(CHANNEL_ID, distributedMediaPackage, elementIds);
          } else {
            Job retractStreamingJob = streamingDistributionService.retract(CHANNEL_ID, distributedMediaPackage,
                elementIds);
            if (retractStreamingJob != null) {
              jobs.add(retractStreamingJob);
            }
          }
        }
      }

      logger.info("Retracting already published Elements for Mediapackage: {}",
          distributedMediaPackage.getIdentifier().toString());
      if (sync) {
        searchService.deleteSynchronously(distributedMediaPackage.getIdentifier().toString());
      } else {
        Job deleteSearchJob = searchService.delete(distributedMediaPackage.getIdentifier().toString());
        if (deleteSearchJob != null) {
          jobs.add(deleteSearchJob);
        }
      }
      // Wait until all retraction jobs have returned
      if (!(sync || !waitForJobs(null, serviceRegistry, jobs.toArray(new Job[jobs.size()])).isSuccess())) {
        throw new PublicationException("One of the retraction jobs did not complete successfully");
      }
    } catch (DistributionException e) {
      throw new PublicationException(e);
    } catch (SearchException e) {
      throw new PublicationException("Error retracting media package", e);
    } catch (UnauthorizedException | NotFoundException ex) {
      logger.error("Retraction of mediapackage failed: {}", distributedMediaPackage.getIdentifier().toString(), ex);
    }
  }

  /**
   * Returns a mediapackage that only contains elements that are marked for distribution.
   *
   * @param current
   *          the current mediapackage
   * @param distributedElements
   *          the distributed mediapackage elements
   * @param downloadSubflavor
   *          flavor to be applied to elements distributed to download
   * @param downloadTargetTags
   *          tags to be applied to elements distributed to downloads
   * @param downloadElementIds
   *          identifiers for elements that have been distributed to downloads
   * @param streamingSubflavor
   *          flavor to be applied to elements distributed to streaming
   * @param streamingElementIds
   *          identifiers for elements that have been distributed to streaming
   * @param streamingTargetTags
   *          tags to be applied to elements distributed to streaming
   * @return the new mediapackage
   */
  private MediaPackage getMediaPackageForSearchIndex(MediaPackage current,
      List<MediaPackageElement> distributedElements, MediaPackageElementFlavor downloadSubflavor,
      String[] downloadTargetTags, Set<String> downloadElementIds, MediaPackageElementFlavor streamingSubflavor,
      Set<String> streamingElementIds, String[] streamingTargetTags) {
    MediaPackage mp = (MediaPackage) current.clone();

    // All the jobs have passed, let's update the mediapackage with references to the distributed elements
    List<String> elementsToPublish = new ArrayList<>();
    Map<String, String> distributedElementIds = new HashMap<>();

    for (MediaPackageElement distributedElement : distributedElements) {

      String sourceElementId = distributedElement.getIdentifier();
      if (sourceElementId != null) {
        MediaPackageElement sourceElement = mp.getElementById(sourceElementId);

        // Make sure the mediapackage is prompted to create a new identifier for this element
        distributedElement.setIdentifier(null);
        if (sourceElement != null) {
          // Adjust the flavor and tags for downloadable elements
          if (downloadElementIds.contains(sourceElementId)) {
            if (downloadSubflavor != null) {
              MediaPackageElementFlavor flavor = sourceElement.getFlavor();
              if (flavor != null) {
                MediaPackageElementFlavor newFlavor = new MediaPackageElementFlavor(flavor.getType(),
                    downloadSubflavor.getSubtype());
                distributedElement.setFlavor(newFlavor);
              }
            }
          } else if (streamingElementIds.contains(sourceElementId)) {
            // Adjust the flavor and tags for streaming elements
            if (streamingSubflavor != null && streamingElementIds.contains(sourceElementId)) {
              MediaPackageElementFlavor flavor = sourceElement.getFlavor();
              if (flavor != null) {
                MediaPackageElementFlavor newFlavor = new MediaPackageElementFlavor(flavor.getType(),
                    streamingSubflavor.getSubtype());
                distributedElement.setFlavor(newFlavor);
              }
            }
          }
          // Copy references from the source elements to the distributed elements
          MediaPackageReference ref = sourceElement.getReference();
          if (ref != null && mp.getElementByReference(ref) != null) {
            MediaPackageReference newReference = (MediaPackageReference) ref.clone();
            distributedElement.setReference(newReference);
          }
        }
      }

      String[] tags = isStreamingFormat(distributedElement) ? streamingTargetTags : downloadTargetTags;
      for (String tag : tags) {
        distributedElement.addTag(tag);
      }

      // Add the new element to the mediapackage
      mp.add(distributedElement);
      elementsToPublish.add(distributedElement.getIdentifier());
      distributedElementIds.put(sourceElementId, distributedElement.getIdentifier());
    }

    // Mark everything that is set for removal
    List<MediaPackageElement> removals = new ArrayList<>();
    for (MediaPackageElement element : mp.getElements()) {
      if (!elementsToPublish.contains(element.getIdentifier())) {
        removals.add(element);
      }
    }

    // Translate references to the distributed artifacts
    for (MediaPackageElement element : mp.getElements()) {

      if (removals.contains(element)) {
        continue;
      }

      // Is the element referencing anything?
      MediaPackageReference reference = element.getReference();
      if (reference == null) {
        continue;
      }

      // See if the element has been distributed
      String distributedElementId = distributedElementIds.get(reference.getIdentifier());
      if (distributedElementId == null) {
        continue;
      }

      MediaPackageReference translatedReference
          = new MediaPackageReferenceImpl(mp.getElementById(distributedElementId));
      if (reference.getProperties() != null) {
        translatedReference.getProperties().putAll(reference.getProperties());
      }

      // Set the new reference
      element.setReference(translatedReference);
    }

    // Remove everything we don't want to add to publish
    for (MediaPackageElement element : removals) {
      mp.remove(element);
    }

    return mp;
  }

  /**
   * Checks if the MediaPackage track transport protocol is a streaming format protocol
   * @param element The MediaPackageElement to analyze
   * @return true if it is a TrackImpl and has a streaming protocol as transport
   */
  private boolean isStreamingFormat(MediaPackageElement element) {
    return element instanceof TrackImpl
        && STREAMING_FORMATS.contains(((TrackImpl) element).getTransport());
  }

  /** Supported streaming formats */
  private static final Set<TrackImpl.StreamingProtocol> STREAMING_FORMATS = new HashSet<>(Arrays.asList(
      TrackImpl.StreamingProtocol.RTMP,
      TrackImpl.StreamingProtocol.RTMPE,
      TrackImpl.StreamingProtocol.HLS,
      TrackImpl.StreamingProtocol.DASH,
      TrackImpl.StreamingProtocol.HDS,
      TrackImpl.StreamingProtocol.SMOOTH));

  /**
   * MH-10216, Copied from the original RepublishWorkflowOperationHandler
   * <p>
   * Merges the updated mediapackage with the one that is currently published in a way where the updated elements
   * replace existing ones in the published mediapackage based on their flavor.
   * <p>
   * If <code>publishedMp</code> is <code>null</code>, this method returns the updated mediapackage without any
   * modifications.
   *
   * @param updatedMp
   *          the updated media package
   * @param publishedMp
   *          the mediapackage that is currently published
   * @return the merged mediapackage
   */
  protected MediaPackage mergePackages(MediaPackage updatedMp, MediaPackage publishedMp,
      List<MediaPackageElementFlavor> mergeForceFlavors, List<MediaPackageElementFlavor> addForceFlavors) {
    if (publishedMp == null) {
      return updatedMp;
    }

    MediaPackage mergedMediaPackage = (MediaPackage) updatedMp.clone();
    for (MediaPackageElement element : publishedMp.elements()) {
      String type = element.getElementType().toString().toLowerCase();
      boolean elementHasFlavorThatAlreadyExists = updatedMp.getElementsByFlavor(element.getFlavor()).length > 0;
      boolean elementHasForceMergeFlavor = mergeForceFlavors.stream().anyMatch((f) -> element.getFlavor().matches(f));
      boolean elementHasForceAddFlavor = addForceFlavors.stream().anyMatch((f) -> element.getFlavor().matches(f));

      if (elementHasForceAddFlavor) {
        logger.info("Adding {} '{}' into the updated mediapackage", type, element.getIdentifier());
        mergedMediaPackage.add((MediaPackageElement) element.clone());
        continue;
      }
      if (!elementHasFlavorThatAlreadyExists) {
        if (elementHasForceMergeFlavor) {
          logger.info("Forcing removal of {} {} due to the absence of a new element with flavor {}",
              type, element.getIdentifier(), element.getFlavor().toString());
          continue;
        }
        logger.info("Merging {} '{}' into the updated mediapackage", type, element.getIdentifier());
        mergedMediaPackage.add((MediaPackageElement) element.clone());
      } else {
        logger.info("Overwriting existing {} '{}' with '{}' in the updated mediapackage",
            type, element.getIdentifier(), updatedMp.getElementsByFlavor(element.getFlavor())[0].getIdentifier());

      }
    }

    return mergedMediaPackage;
  }

  @Activate
  public void activate(ComponentContext cc) {
    BundleContext bundleContext = cc.getBundleContext();

    // Get configuration
    serverUrl = UrlSupport.url(bundleContext.getProperty(SERVER_URL_PROPERTY));
    publishedStreamingFormats = Arrays.asList(Optional.ofNullable(StringUtils.split(
        bundleContext.getProperty(STREAMING_PUBLISH_PROPERTY), ",")).orElse(new String[0]));
  }

  static final String STREAMING_PUBLISH_PROPERTY = "org.opencastproject.publish.streaming.formats";

  private URL serverUrl;

  private List<String> publishedStreamingFormats;

  private ServiceRegistry serviceRegistry;

  @Reference
  public void setServiceRegistry(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }
}
