import pandas as pd
from typing import Union, Optional
from pathlib import Path
import logging
import semver

from allensdk.brain_observatory.behavior.behavior_session import (
    BehaviorSession)
from allensdk.api.cloud_cache.cloud_cache import (
    S3CloudCache, LocalCache, StaticLocalCache)

from allensdk.brain_observatory.ecephys.behavior_ecephys_session \
    import BehaviorEcephysSession

# [min inclusive, max exclusive)
MANIFEST_COMPATIBILITY = ["0.1.0", "1.0.0"]


class BehaviorCloudCacheVersionException(Exception):
    pass


def version_check(manifest_version: str,
                  data_pipeline_version: str,
                  cmin: str = MANIFEST_COMPATIBILITY[0],
                  cmax: str = MANIFEST_COMPATIBILITY[1]):
    mver_parsed = semver.VersionInfo.parse(manifest_version)
    cmin_parsed = semver.VersionInfo.parse(cmin)
    cmax_parsed = semver.VersionInfo.parse(cmax)
    if (mver_parsed < cmin_parsed) | (mver_parsed >= cmax_parsed):
        estr = (f"the manifest has manifest_version {manifest_version} but "
                "this version of AllenSDK is compatible only with manifest "
                f"versions {cmin} <= X < {cmax}. \n"
                "Consider using a version of AllenSDK closer to the version "
                f"used to release the data: {data_pipeline_version}")
        raise BehaviorCloudCacheVersionException(estr)


class BehaviorEcephysProjectCloudApi():
    """API for downloading data released on S3 and returning tables.

    Parameters
    ----------
    cache: S3CloudCache
        an instantiated S3CloudCache object, which has already run
        `self.load_manifest()` which populates the columns:
          - metadata_file_names
          - file_id_column
    skip_version_check: bool
        whether to skip the version checking of pipeline SDK version
        vs. running SDK version, which may raise Exceptions. (default=False)
    local: bool
        Whether to operate in local mode, where no data will be downloaded
        and instead will be loaded from local
    """
    def __init__(
        self,
        cache: Union[S3CloudCache, LocalCache, StaticLocalCache],
        skip_version_check: bool = False,
        local: bool = False
    ):

        self.cache = cache
        self.skip_version_check = skip_version_check
        self._local = local
        self.load_manifest()

    def load_manifest(self, manifest_name: Optional[str] = None):
        """
        Load the specified manifest file into the CloudCache

        Parameters
        ----------
        manifest_name: Optional[str]
            Name of manifest file to load. If None, load latest
            (default: None)
        """
        if manifest_name is None:
            self.cache.load_last_manifest()
        else:
            self.cache.load_manifest(manifest_name)

        expected_metadata = set(["behavior_sessions",
                                 "sessions",
                                 "probes",
                                 "units",
                                 "channels"])

        if self.cache._manifest.metadata_file_names is None:
            raise RuntimeError("S3CloudCache object has no metadata "
                               "file names. BehaviorProjectCloudApi "
                               "expects a S3CloudCache passed which "
                               "has already run load_manifest()")
        cache_metadata = set(self.cache._manifest.metadata_file_names)

        if cache_metadata != expected_metadata:
            raise RuntimeError("expected S3CloudCache object to have "
                               f"metadata file names: {expected_metadata} "
                               f"but it has {cache_metadata}")

        if not self.skip_version_check:
            data_sdk_version = [i for i in self.cache._manifest._data_pipeline
                                if i['name'] == "AllenSDK"][0]["version"]
            version_check(self.cache._manifest.version, data_sdk_version)

        self.logger = logging.getLogger("BehaviorEcephysProjectCloudApi")
        self._get_ecephys_session_table()
        self._get_behavior_session_table()
        self._get_unit_table()
        self._get_probe_table()
        self._get_channel_table()

    @classmethod
    def from_s3_cache(cls, cache_dir: Union[str, Path],
                      bucket_name: str,
                      project_name: str,
                      ui_class_name: str) -> "BehaviorEcephysProjectCloudApi":
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

        ui_class_name: str
            Name of user interface class (used to populate error messages)

        Returns
        -------
        BehaviorProjectCloudApi instance

        """
        cache = S3CloudCache(cache_dir,
                             bucket_name,
                             project_name,
                             ui_class_name=ui_class_name)
        return cls(cache)

    @staticmethod
    def from_local_cache(
        cache_dir: Union[str, Path],
        project_name: str,
        ui_class_name: str,
        use_static_cache: bool = False
    ) -> "BehaviorEcephysProjectCloudApi":
        """instantiates this object with a local cache.

        Parameters
        ----------
        cache_dir: str or pathlib.Path
            Path to the directory where data will be stored on the local system

        project_name: str
            the name of the project this cache is supposed to access. This
            project name is the first part of the prefix of the release data
            objects. I.e. s3://<bucket_name>/<project_name>/<object tree>

        ui_class_name: str
            Name of user interface class (used to populate error messages)

        Returns
        -------
        BehaviorProjectCloudApi instance

        """
        if use_static_cache:
            cache = StaticLocalCache(
                cache_dir,
                project_name,
                ui_class_name=ui_class_name
            )
        else:
            cache = LocalCache(
                cache_dir,
                project_name,
                ui_class_name=ui_class_name
            )
        return BehaviorEcephysProjectCloudApi(cache, local=True)

    def get_behavior_session(
            self, behavior_session_id: int) -> BehaviorSession:
        """get a BehaviorSession by specifying behavior_session_id

        Parameters
        ----------
        behavior_session_id: int
            the id of the behavior_session

        Returns
        -------
        BehaviorSession

        Notes
        -----
        behavior session does not include file_id.
        The file id is accessed via ecephys_session_id key
        from the ecephys_session_table
        """
        row = self._behavior_session_table.query(
                f"behavior_session_id=={behavior_session_id}")
        if row.shape[0] != 1:
            raise RuntimeError("The behavior_session_table should have "
                               "1 and only 1 entry for a given "
                               "behavior_session_id. For "
                               f"{behavior_session_id} "
                               f" there are {row.shape[0]} entries.")
        row = row.squeeze()
        esid = row.ecephys_session_id
        row = self._ecephys_session_table.query(f"index=={esid}")
        file_id = str(int(row[self.cache.file_id_column]))
        data_path = self._get_data_path(file_id=file_id)
        return BehaviorSession.from_nwb_path(str(data_path))

    def get_ecephys_session(
        self,
        ecephys_session_id: int
    ) -> BehaviorEcephysSession:

        """get a BehaviorEcephysSession by specifying ecephys_session_id

        Parameters
        ----------
        ecephys_session_id: int
            the id of the ecephys session

        Returns
        -------
        BehaviorEcephysSession

        """
        row = self._ecephys_session_table.query(
                f"index=={ecephys_session_id}")
        if row.shape[0] != 1:
            raise RuntimeError("The behavior_ecephys_session_table should "
                               "have 1 and only 1 entry for a given "
                               f"ecephys_session_id. For "
                               f"{ecephys_session_id} "
                               f" there are {row.shape[0]} entries.")
        file_id = str(int(row[self.cache.file_id_column]))
        data_path = self._get_data_path(file_id=file_id)
        return BehaviorEcephysSession.from_nwb_path(
            str(data_path))

    def _get_ecephys_session_table(self):
        session_table_path = self._get_metadata_path(
            fname="sessions")
        df = pd.read_csv(session_table_path)
        self._ecephys_session_table = df.set_index("ecephys_session_id")

    def get_ecephys_session_table(self) -> pd.DataFrame:
        """Return a pd.Dataframe table summarizing ecephys_sessions
        and associated metadata.

        """
        return self._ecephys_session_table

    def _get_behavior_session_table(self):
        session_table_path = self._get_metadata_path(
            fname='behavior_sessions')
        df = pd.read_csv(session_table_path)
        self._behavior_session_table = df.set_index("behavior_session_id")

    def get_behavior_session_table(self) -> pd.DataFrame:
        return self._behavior_session_table

    def _get_probe_table(self):
        probe_table_path = self._get_metadata_path(
            fname="probes")
        df = pd.read_csv(probe_table_path)
        self._probe_table = df.set_index("ecephys_probe_id")

    def get_probe_table(self):
        return self._probe_table

    def _get_unit_table(self):
        unit_table_path = self._get_metadata_path(
            fname="units")
        df = pd.read_csv(unit_table_path)
        self._unit_table = df.set_index("unit_id")

    def get_unit_table(self):
        return self._unit_table

    def _get_channel_table(self):
        channel_table_path = self._get_metadata_path(
            fname="channels")
        df = pd.read_csv(channel_table_path)
        self._channel_table = df.set_index("ecephys_channel_id")

    def get_channel_table(self):
        return self._channel_table

    def _get_metadata_path(self, fname: str):
        if self._local:
            path = self._get_local_path(fname=fname)
        else:
            path = self.cache.download_metadata(fname=fname)
        return path

    def _get_data_path(self, file_id: str):
        if self._local:
            data_path = self._get_local_path(file_id=file_id)
        else:
            data_path = self.cache.download_data(file_id=file_id)
        return data_path

    def _get_local_path(self, fname: Optional[str] = None, file_id:
                        Optional[str] = None):
        if fname is None and file_id is None:
            raise ValueError('Must pass either fname or file_id')

        if fname is not None and file_id is not None:
            raise ValueError('Must pass only one of fname or file_id')

        if fname is not None:
            path = self.cache.metadata_path(fname=fname)
        else:
            path = self.cache.data_path(file_id=file_id)

        exists = path['exists']
        local_path = path['local_path']
        if not exists:
            raise FileNotFoundError(f'You started a cache without a '
                                    f'connection to s3 and {local_path} is '
                                    'not already on your system')
        return local_path
