from functools import partial
from typing import Optional, Union
from pathlib import Path
import logging

from allensdk.brain_observatory.behavior.behavior_project_cache.project_apis.data_io import (  # noqa: E501
    BehaviorEcephysProjectCloudApi)
from allensdk.api.warehouse_cache.caching_utilities import call_caching


class VisualBehaviorEcephysProjectCache(object):

    def __init__(
            self,
            fetch_api: Optional[BehaviorEcephysProjectCloudApi] = None,
            fetch_tries: int = 2,
            ):
        """ Entrypoint for accessing Visual Behavior Neuropixels data.

        Supports access to metadata tables:
        get_ecephys_session_table()
        get_behavior_session_table()
        get_probe_table()
        get_channel_table()
        get_unit_table

        Provides methods for instantiating session objects
        from the nwb files:
        get_ecephys_session() to load BehaviorEcephysSession
        get_behavior_sesion() to load BehaviorSession

        Provides tools for downloading data:

        Will download data from the s3 bucket if session nwb file is not
        in the local cache, othwerwise will use file from the cache.

        Parameters
        ==========
        fetch_api :
            Used to pull data from remote sources, after which it is locally
            cached.
        fetch_tries :
            Maximum number of times to attempt a download before giving up and
            raising an exception. Note that this is total tries, not retries.
            Default=2.
        """

        self.fetch_api = fetch_api
        self.cache = None

        self.fetch_tries = fetch_tries
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def manifest(self):
        if self.cache is None:
            api_name = type(self.fetch_api).__name__
            raise NotImplementedError(f"A {type(self).__name__} "
                                      f"based on {api_name} "
                                      "does not have an accessible manifest "
                                      "property")
        return self.cache.manifest

    @classmethod
    def from_s3_cache(cls, cache_dir: Union[str, Path],
                      bucket_name: str = "visual-behavior-ecephys-data",
                      project_name: str = "visual-behavior-ecephys"
                      ) -> "VisualBehaviorEcephysProjectCache":
        """instantiates this object with a connection to an s3 bucket and/or
        a local cache related to that bucket.

        Parameters
        ----------
        cache_dir: str or pathlib.Path
            Path to the directory where data will be stored on the local system

        bucket_name: str
            for example, if bucket URI is 's3://mybucket' this value should be
            'mybucket'

        project_name: str
            the name of the project this cache is supposed to access. This
            project name is the first part of the prefix of the release data
            objects. I.e. s3://<bucket_name>/<project_name>/<object tree>

        Returns
        -------
        VisualBehaviorEcephysProjectCache instance

        """
        fetch_api = BehaviorEcephysProjectCloudApi.from_s3_cache(
                cache_dir, bucket_name, project_name,
                ui_class_name=cls.__name__)

        return cls(fetch_api=fetch_api)

    @classmethod
    def from_local_cache(
        cls,
        cache_dir: Union[str, Path],
        project_name: str = "visual-behavior-ecephys",
        use_static_cache: bool = False
    ) -> "VisualBehaviorEcephysProjectCache":
        """instantiates this object with a local cache.

        Parameters
        ----------
        cache_dir: str or pathlib.Path
            Path to the directory where data will be stored on the local system

        project_name: str
            the name of the project this cache is supposed to access. This
            project name is the first part of the prefix of the release data
            objects. I.e. s3://<bucket_name>/<project_name>/<object tree>

        Returns
        -------
        VisualBehaviorEcephysProjectCache instance

        """
        fetch_api = BehaviorEcephysProjectCloudApi.from_local_cache(
            cache_dir,
            project_name,
            ui_class_name=cls.__name__,
            use_static_cache=use_static_cache
        )
        return cls(fetch_api=fetch_api)

    def _cache_not_implemented(self, method_name: str) -> None:
        """
        Raise a NotImplementedError explaining that method_name
        does not exist for VisualBehaviorEcephysProjectCache
        that does not have a fetch_api based on LIMS
        """
        msg = f"Method {method_name} does not exist for this "
        msg += f"{type(self).__name__}, which is based on "
        msg += f"{type(self.fetch_api).__name__}"
        raise NotImplementedError(msg)

    def construct_local_manifest(self) -> None:
        """
        Construct the local file used to determine if two files are
        duplicates of each other or not. Save it into the expected
        place in the cache. (You will see a warning if the cache
        thinks that you need to run this method).
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('construct_local_manifest')
        self.fetch_api.cache.construct_local_manifest()

    def compare_manifests(self,
                          manifest_0_name: str,
                          manifest_1_name: str
                          ) -> str:
        """
        Compare two manifests from this dataset. Return a dict
        containing the list of metadata and data files that changed
        between them

        Note: this assumes that manifest_0 predates manifest_1

        Parameters
        ----------
        manifest_0_name: str

        manifest_1_name: str

        Returns
        -------
        str
            A string summarizing all of the changes going from
            manifest_0 to manifest_1
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('compare_manifests')
        return self.fetch_api.cache.compare_manifests(manifest_0_name,
                                                      manifest_1_name)

    def load_latest_manifest(self) -> None:
        """
        Load the manifest corresponding to the most up to date
        version of the dataset.
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('load_latest_manifest')
        self.fetch_api.cache.load_latest_manifest()

    def latest_downloaded_manifest_file(self) -> str:
        """
        Return the name of the most up to date data manifest
        available on your local system.
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('latest_downloaded_manifest_file')
        return self.fetch_api.cache.latest_downloaded_manifest_file

    def latest_manifest_file(self) -> str:
        """
        Return the name of the most up to date data manifest
        corresponding to this dataset, checking in the cloud
        if this is a cloud-backed cache.
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('latest_manifest_file')
        return self.fetch_api.cache.latest_manifest_file

    def load_manifest(self, manifest_name: str):
        """
        Load a specific versioned manifest for this dataset.

        Parameters
        ----------
        manifest_name: str
            The name of the manifest to load. Must be an element in
            self.manifest_file_names
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('load_manifest')
        self.fetch_api.load_manifest(manifest_name)

    def list_all_downloaded_manifests(self) -> list:
        """
        Return a sorted list of the names of the manifest files
        that have been downloaded to this cache.
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('list_all_downloaded_manifests')
        return self.fetch_api.cache.list_all_downloaded_manifests()

    def list_manifest_file_names(self) -> list:
        """
        Return a sorted list of the names of the manifest files
        associated with this dataset.
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('list_manifest_file_names')
        return self.fetch_api.cache.manifest_file_names

    def current_manifest(self) -> Union[None, str]:
        """
        Return the name of the dataset manifest currently being
        used by this cache.
        """
        if not isinstance(self.fetch_api, BehaviorEcephysProjectCloudApi):
            self._cache_not_implemented('current_manifest')
        return self.fetch_api.cache.current_manifest

    def get_ecephys_session_table(self):
        return self.fetch_api.get_ecephys_session_table(),

    def get_behavior_session_table(self):
        return self.fetch_api.get_behavior_session_table(),

    def get_probe_table(self):
        self.fetch_api.get_probe_table(),

    def get_channel_table(self):
        return self.fetch_api.get_channel_table(),

    def get_unit_table(self):
        return self.fetch_api.get_unit_table(),

    def get_ecephys_session(self, ecephys_session_id: int):

        fetch_session = partial(self.fetch_api.get_ecephys_session,
                                ecephys_session_id)
        return call_caching(
            fetch_session,
            lambda x: x,  # not writing anything
            lazy=False,  # can't actually read from file cache
            read=fetch_session
        )

    def get_behavior_session(self, behavior_session_id: int):

        fetch_session = partial(self.fetch_api.get_behavior_session,
                                behavior_session_id)
        return call_caching(
            fetch_session,
            lambda x: x,  # not writing anything
            lazy=False,  # can't actually read from file cache
            read=fetch_session
        )
