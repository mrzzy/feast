# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from collections import OrderedDict, defaultdict
from typing import Dict
from typing import List

import grpc
import numpy as np
import pandas as pd
from google.cloud import storage
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionRequest,
    GetFeatureSetsResponse,
    ApplyFeatureSetRequest,
    GetFeatureSetsRequest,
    ApplyFeatureSetResponse,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.feature_set import FeatureSet, Entity
from feast.job import Job
from feast.serving.ServingService_pb2 import (
    GetFeaturesRequest,
    GetFeastServingVersionRequest,
    GetOnlineFeaturesResponse,
)
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.type_map import pandas_value_to_proto_value, FEAST_VALUE_ATTR_TO_DTYPE
from feast.types.Value_pb2 import Value

GRPC_CONNECTION_TIMEOUT_DEFAULT = 5  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 300  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300


class Client:
    def __init__(
        self, core_url: str = None, serving_url: str = None, verbose: bool = False
    ):
        self._core_url = core_url
        self._serving_url = serving_url
        self._verbose = verbose
        self.__core_channel: grpc.Channel = None
        self.__serving_channel: grpc.Channel = None
        self._core_service_stub: CoreServiceStub = None
        self._serving_service_stub: ServingServiceStub = None
        # TODO: Do not instantiate storage_client here
        self._storage_client: storage.Client = storage.Client()

    @property
    def core_url(self) -> str:
        if self._core_url is not None:
            return self._core_url
        if os.getenv(FEAST_CORE_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_CORE_URL_ENV_KEY)
        return ""

    @core_url.setter
    def core_url(self, value: str):
        self._core_url = value

    @property
    def serving_url(self) -> str:
        if self._serving_url is not None:
            return self._serving_url
        if os.getenv(FEAST_SERVING_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_SERVING_URL_ENV_KEY)
        return ""

    @serving_url.setter
    def serving_url(self, value: str):
        self._serving_url = value

    def version(self):
        self._connect_core()
        self._connect_serving()

        try:
            core_version = self._core_service_stub.GetFeastCoreVersion(
                GetFeastCoreVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
        except grpc.FutureCancelledError:
            core_version = "not connected"

        try:
            serving_version = self._serving_service_stub.GetFeastServingVersion(
                GetFeastServingVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
        except grpc.FutureCancelledError:
            serving_version = "not connected"

        return {
            "core": {"url": self.core_url, "version": core_version},
            "serving": {"url": self.serving_url, "version": serving_version},
        }

    def _connect_core(self, skip_if_connected=True):
        """
        Connect to Core API
        """
        if skip_if_connected and self._core_service_stub:
            return

        if not self.core_url:
            raise ValueError("Please set Feast Core URL.")

        if self.__core_channel is None:
            self.__core_channel = grpc.insecure_channel(self.core_url)

        try:
            grpc.channel_ready_future(self.__core_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                "connection timed out while attempting to connect to Feast Core gRPC server "
                + self.core_url
            )
        else:
            self._core_service_stub = CoreServiceStub(self.__core_channel)

    def _connect_serving(self, skip_if_connected=True):
        """
        Connect to Serving API
        """

        if skip_if_connected and self._serving_service_stub:
            return

        if not self.serving_url:
            raise ValueError("Please set Feast Serving URL.")

        if self.__serving_channel is None:
            self.__serving_channel = grpc.insecure_channel(self.serving_url)

        try:
            grpc.channel_ready_future(self.__serving_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                "connection timed out while attempting to connect to Feast Serving gRPC server "
                + self.serving_url
            )
        else:
            self._serving_service_stub = ServingServiceStub(self.__serving_channel)

    def apply(self, resources):
        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if isinstance(resource, FeatureSet):
                self._apply_feature_set(resource)
                continue
            raise Exception("Could not determine resource type to apply")

    @property
    def feature_sets(self) -> List[FeatureSet]:
        """
        Retrieve a list of Feature Sets from Feast Core
        """
        self._connect_core(skip_if_connected=True)

        # Get latest Feature Sets from Feast Core
        feature_set_protos = self._core_service_stub.GetFeatureSets(
            GetFeatureSetsRequest()
        )  # type: GetFeatureSetsResponse

        # Store list of Feature Sets
        feature_sets = []
        for feature_set_proto in feature_set_protos.feature_sets:
            feature_set = FeatureSet.from_proto(feature_set_proto)
            feature_set._client = self
            feature_sets.append(feature_set)
        return feature_sets

    def get_feature_set(self, name: str, version: int) -> FeatureSet:
        """
        Retrieve a single Feature Set from Feast Core
        """
        self._connect_core(skip_if_connected=True)

        get_feature_set_response = self._core_service_stub.GetFeatureSets(
            GetFeatureSetsRequest(
                filter=GetFeatureSetsRequest.Filter(
                    feature_set_name=name.strip(), feature_set_version=str(version)
                )
            )
        )  # type: GetFeatureSetsResponse

        num_feature_sets_found = len(list(get_feature_set_response.feature_sets))

        if num_feature_sets_found == 0:
            return

        if num_feature_sets_found > 1:
            raise Exception(
                f'Found {num_feature_sets_found} feature sets with name "{name}"'
                f' and version "{version}".'
            )

        return FeatureSet.from_proto(get_feature_set_response.feature_sets[0])

    @property
    def entities(self) -> Dict[str, Entity]:
        entities_dict = OrderedDict()
        for fs in self.feature_sets:
            for entity in fs.entities:
                entities_dict[entity.name] = entity
        return entities_dict

    def _apply_feature_set(self, feature_set: FeatureSet):
        self._connect_core(skip_if_connected=True)
        feature_set._client = self

        apply_fs_response = self._core_service_stub.ApplyFeatureSet(
            ApplyFeatureSetRequest(feature_set=feature_set.to_proto()),
            timeout=GRPC_CONNECTION_TIMEOUT_APPLY,
        )  # type: ApplyFeatureSetResponse

        if apply_fs_response.status == ApplyFeatureSetResponse.Status.ERROR:
            raise Exception(
                "Error while trying to apply feature set " + feature_set.name
            )

        applied_fs = FeatureSet.from_proto(apply_fs_response.feature_set)
        feature_set._update_from_feature_set(applied_fs, is_dirty=False)
        return

    def get(
        self,
        entity_data: pd.DataFrame,
        feature_ids: List[str],
        join_on: Dict[str, str] = None,
        batch: bool = False,
    ) -> pd.DataFrame:
        self._connect_serving(skip_if_connected=True)

        if "datetime" != entity_data.columns[0]:
            raise ValueError("The first column in entity_data should be 'datetime'")

        entity_names = []
        for column in entity_data.columns[1:]:
            entity_names.append(column)

        entity_dataset_rows = entity_data.apply(
            _convert_to_proto_value_fn(entity_data.dtypes), axis=1
        )

        feature_set_request = create_feature_set_request_from_feature_strings(
            feature_ids
        )

        if batch:
            return self.get_batch_features(
                entity_data, entity_dataset_rows, entity_names, feature_set_request
            )

        if not batch:
            return self.get_online_features(
                entity_data, entity_dataset_rows, entity_names, feature_set_request
            )

    def get_batch_features(
        self, feature_ids: List[str], entity_data: pd.DataFrame
    ) -> Job:
        """

        Args:
            feature_ids: List of feature id. Feature id is in this format
                         "feature_set_name:version:feature_name".

            entity_data: Pandas Dataframe representing the requested features.

                         The first column must be named "datetime" with dtype
                         "datetime64[ns]" or "datetime64[ns],UTC".

                         Subsequent columns representing the entity ids requested
                         must have the correct dtypes corresponding to the fields
                         in the features set spec.

                         Example dataframe:

                         datetime   | entity_id_1 | entity_id_2
                         --------------------------------------
                         1570154927 | 89          | 23

        Returns:
            Feast batch retrieval job: feast.job.Job
            
        Example usage:
        ============================================================
        feast_client = Client(core_url="localhost:6565", serving_url="localhost:6566")
        feature_ids = ["driver:1:city"]
        entity_data = pd.DataFrame({"datetime": [datetime.utcnow()], "driver_id": [np.nan]})
        feature_retrieval_job = feast_client.get_batch_features(feature_ids, entity_data)
        df = feature_retrieval_job.to_dataframe()
        print(df)
        """

        # We feature_set and entity_rows to construct a GetFeaturesRequest object
        # and send it to Feast Serving GRPC server
        feature_sets = []
        entity_rows = []

        # Dict of feature_set_name:version (fsv) -> list of feature_names (fn)
        # This is a variable to help construct feature_sets
        fsv_to_fns = defaultdict(list)

        # TODO: Perform validation on the provided features_ids

        for feature_id in feature_ids:
            feature_set_name, version_str, feature_name = feature_id.split(":")
            fsv_to_fns[f"{feature_set_name}:{version_str}"].append(feature_name)

        # TODO: Do not harcode max_age but allow users to specify it in the parameters
        hardcoded_max_age_sec = 604800  # 1 week

        for fsv, feature_names in fsv_to_fns.items():
            feature_set_name, version_str = fsv.split(":")
            feature_sets.append(
                GetFeaturesRequest.FeatureSet(
                    name=feature_set_name,
                    version=int(version_str),
                    feature_names=feature_names,
                    max_age=Duration(seconds=hardcoded_max_age_sec),
                )
            )

        for index, row in entity_data.iterrows():
            # Fields is a map of entity_id to Feast Value
            fields = {}

            # TODO: Support more dtypes

            for entity_id, dtype in entity_data.dtypes.items():
                if pd.isnull(row[entity_id]):
                    # Handle unset i.e. np.nan value
                    fields[entity_id] = Value()
                elif entity_id == "datetime":
                    continue
                elif dtype == "int64":
                    fields[entity_id] = Value(int64_val=row[entity_id])
                elif dtype == "int32":
                    fields[entity_id] = Value(int32_val=row[entity_id])
                elif dtype == "object":
                    fields[entity_id] = Value(string_val=row[entity_id])
                elif dtype == "float64":
                    fields[entity_id] = Value(float_val=row[entity_id])
                else:
                    raise Exception("Unsupported dtype for now: " + str(dtype))

            entity_rows.append(
                GetFeaturesRequest.EntityRow(
                    entity_timestamp=Timestamp(
                        seconds=np.datetime64(row.datetime).astype("int64") // 1000000
                    ),
                    fields=fields,
                )
            )

        request = GetFeaturesRequest(feature_sets=feature_sets, entity_rows=entity_rows)

        # TODO: Move this out from this method
        if self._serving_service_stub is None:
            self._serving_service_stub = channel = grpc.insecure_channel(
                self.serving_url
            )
            self._serving_service_stub = ServingServiceStub(channel)

        response = self._serving_service_stub.GetBatchFeatures(request)
        return Job(response.job, self._serving_service_stub, self._storage_client)

    def get_batch_features_old(self, feature_sets, entity_rows):
        request = GetFeaturesRequest(feature_sets=feature_sets, entity_rows=entity_rows)
        response = self._serving_service_stub.GetBatchFeatures(request)
        return Job(response.job, self._serving_service_stub, self._storage_client)

    def get_online_features(
        self, entity_data, entity_dataset_rows, entity_names, feature_set_request
    ):
        get_online_features_response_proto = self._serving_service_stub.GetOnlineFeatures(
            GetFeaturesRequest(
                entity_dataset=GetFeaturesRequest.EntityDataset(
                    entity_dataset_rows=entity_dataset_rows, entity_names=entity_names
                ),
                feature_sets=feature_set_request,
            )
        )  # type: GetOnlineFeaturesResponse
        feature_dataframe = feature_data_sets_to_pandas_dataframe(
            entity_data_set=entity_data.copy(),
            feature_data_sets=list(get_online_features_response_proto.feature_datasets),
        )
        return feature_dataframe


def _convert_to_proto_value_fn(dtypes: pd.core.generic.NDFrame):
    def convert_to_proto_value(row: pd.Series):
        entity_dataset_row = GetFeaturesRequest.EntityDatasetRow()
        for i in range(len(row) - 1):
            entity_dataset_row.entity_ids.append(
                pandas_value_to_proto_value(dtypes[i + 1], row[i + 1])
            )
        return entity_dataset_row

    return convert_to_proto_value


# TODO: Update the signature to updated protos
def feature_data_sets_to_pandas_dataframe(
    entity_data_set: pd.DataFrame, feature_data_sets
):
    feature_data_set_dataframes = []
    for feature_data_set in feature_data_sets:
        # Validate feature data set length
        if len(feature_data_set.feature_rows) != len(entity_data_set.index):
            raise Exception(
                "Feature data set response is of different size "
                + str(len(feature_data_set.feature_rows))
                + " than the entity data set request "
                + str(len(entity_data_set.index))
            )

        # Convert to Pandas DataFrame
        feature_data_set_dataframes.append(
            feature_data_set_to_pandas_dataframe(feature_data_set)
        )

    # Join dataframes into a single feature dataframe
    dataframe = join_feature_set_dataframes(feature_data_set_dataframes)
    return dataframe


def join_feature_set_dataframes(
    feature_data_set_dataframes: List[pd.DataFrame]
) -> pd.DataFrame:
    return (
        feature_data_set_dataframes[0]
        if len(feature_data_set_dataframes) > 0
        else pd.DataFrame
    )


# TODO: Update method signature to updated protos
def feature_data_set_to_pandas_dataframe(feature_data_set) -> pd.DataFrame:
    feature_set_name = feature_data_set.name
    dtypes = {}
    value_attr = {}
    columns = []
    data = {}
    first_run_done = False

    for featureRow in feature_data_set.feature_rows:
        for field in featureRow.fields:
            feature_id = feature_set_name + "." + field.name

            if not first_run_done:
                columns.append(feature_id)
                data[feature_id] = []
                value_attr[feature_id] = field.value.WhichOneof("val")
                dtypes[feature_id] = FEAST_VALUE_ATTR_TO_DTYPE[value_attr[feature_id]]

            if not field.value.HasField(value_attr[feature_id]):
                data[feature_id].append(None)
            else:
                data[feature_id].append(getattr(field.value, value_attr[feature_id]))

        first_run_done = True

    dataframe = (
        pd.DataFrame(columns=columns, data=data).reset_index(drop=True).astype(dtypes)
    )

    return dataframe


def create_feature_set_request_from_feature_strings(
    feature_ids: List[str]
) -> List[GetFeaturesRequest.FeatureSet]:
    feature_set_request = dict()  # type: Dict[str, GetFeaturesRequest.FeatureSet]
    for feature_id in feature_ids:
        feature_set, feature = feature_id.split(".")
        if feature_set not in feature_set_request:
            feature_set_name, feature_set_version = feature_set.split(":")
            feature_set_request[feature_set] = GetFeaturesRequest.FeatureSet(
                name=feature_set_name, version=int(feature_set_version)
            )
        feature_set_request[feature_set].feature_names.append(feature)
    return list(feature_set_request.values())