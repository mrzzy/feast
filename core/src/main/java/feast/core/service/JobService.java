/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.core.service;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.core.CoreServiceProto.ListIngestionJobsRequest;
import feast.core.CoreServiceProto.ListIngestionJobsResponse;
import feast.core.FeatureSetReferenceProto.FeatureSetReference;
import feast.core.IngestionJobProto;
import feast.core.dao.FeatureSetRepository;
import feast.core.dao.JobRepository;
import feast.core.job.JobManager;
import feast.core.job.Runner;
import feast.core.model.FeatureSet;
import feast.core.model.Job;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Defines a Job Managemenent Service that allows users to manage feast ingestion jobs. */
@Service
public class JobService {
  private JobRepository jobRepository;
  private FeatureSetRepository featureSetRepository;
  private Map<Runner, JobManager> jobManagers;

  @Autowired
  public JobService(
      JobRepository jobRepository,
      FeatureSetRepository featureSetRepository,
      List<JobManager> jobManagerList) {
    this.jobRepository = jobRepository;
    this.featureSetRepository = featureSetRepository;

    for (JobManager manager : jobManagerList) {
      this.jobManagers.put(manager.getRunnerType(), manager);
    }
  }

  /* Service API */
  /**
   * List Ingestion Jobs in feast matching the given filter -
   *
   * @param filter to use to filter match against ingestion jobs
   * @throws UnsupportedOperationException when given a filter in a unsupported configuration
   * @throws InvalidProtocolBufferException if an error occurred when constructing ingestion job
   *     protobuf
   * @return list ingestion jobs response
   */
  public ListIngestionJobsResponse listJobs(ListIngestionJobsRequest.Filter filter)
      throws InvalidProtocolBufferException, UnsupportedOperationException {
    // filter jobs based on request filter
    Set<Job> matchingJobs = new HashSet<>();

    // for proto3, default value for missing values:
    // - numeric values (ie int) is zero
    // - strings is empty string

    if (filter.getId() != "") {
      // get by id: no more filters required: found job
      Optional<Job> job = this.jobRepository.findById(filter.getId());
      if (job.isPresent()) {
        matchingJobs.add(job.get());
      }

    } else {
      // multiple filters can apply together in an 'and' operation
      if (filter.getStoreName() != "") {
        // find jobs by name
        Collection<Job> jobs = this.jobRepository.findByStoreName(filter.getStoreName());
        matchingJobs = this.mergeResults(matchingJobs, jobs);
      }
      if (filter.hasFeatureSetReference()) {
        // find a matching featureset for reference
        FeatureSetReference fsReference = filter.getFeatureSetReference();
        List<FeatureSet> matchFeatureSets = this.findFeatureSets(fsReference);
        Collection<Job> jobs = this.jobRepository.findByFeatureSetIn(matchFeatureSets);
        matchingJobs = this.mergeResults(matchingJobs, jobs);
      }
    }

    // convert matching job models to ingestion job protos
    List<IngestionJobProto.IngestionJob> ingestJobs = new ArrayList<>();
    for (Job job : matchingJobs) {
      ingestJobs.add(job.toIngestionProto());
    }

    // pack jobs into response
    return ListIngestionJobsResponse.newBuilder().addAllJobs(ingestJobs).build();
  }

  /* Private Utility Methods */
  /**
   * Finds &amp; returns featuresets matching the given feature set refererence
   *
   * @param fsReference FeatureSetReference that specifies which featuresets to match
   * @throws UnsupportedOperationException fsReference given is unsupported.
   * @return Returns a list of matching featuresets
   */
  private List<FeatureSet> findFeatureSets(FeatureSetReference fsReference)
      throws UnsupportedOperationException {

    String fsName = fsReference.getName();
    String fsProject = fsReference.getProject();
    Integer fsVersion = fsReference.getVersion();

    List<FeatureSet> featureSets = new ArrayList<>();
    if (fsName != "" && fsProject != "" && fsVersion != 0) {
      featureSets.add(
          this.featureSetRepository.findFeatureSetByNameAndProject_NameAndVersion(
              fsName, fsProject, fsVersion));
    } else if (fsName != "" && fsProject != "") {
      featureSets.addAll(this.featureSetRepository.findAllByNameAndProject_Name(fsName, fsProject));
    } else if (fsName != "" && fsVersion != 0) {
      featureSets.addAll(this.featureSetRepository.findAllByNameAndVersion(fsName, fsVersion));
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported featureset refererence configuration: "
                  + "(name: '%s', project: '%s', version: '%d')",
              fsName, fsProject, fsVersion));
    }

    return featureSets;
  }

  private <T> Set<T> mergeResults(Set<T> results, Collection<T> newResults) {
    if (results.size() <= 0) {
      // no existing results: copy over new results
      results.addAll(newResults);
    } else {
      // and operation: keep results that exist in both existing and new results
      results.retainAll(newResults);
    }
    return results;
  }
}
