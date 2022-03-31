from typing import Optional

from allensdk.brain_observatory.behavior.data_objects.base \
    .readable_interfaces import \
    JsonReadableInterface, LimsReadableInterface, NwbReadableInterface
from allensdk.brain_observatory.behavior.data_objects.base \
    .writable_interfaces import \
    JsonWritableInterface, NwbWritableInterface
from allensdk.brain_observatory.behavior.data_objects import (
    DataObject
)

import pandas as pd

from allensdk.brain_observatory.behavior.data_objects.\
    running_speed.running_speed import RunningSpeedNWBMixin

from allensdk.brain_observatory.ecephys.data_objects.\
    running_speed.multi_stim_running_processing import (
        multi_stim_running_df_from_raw_data)


def _get_multi_stim_running_df(
        sync_file: str,
        behavior_stimulus_file: str,
        mapping_stimulus_file: str,
        replay_stimulus_file: str,
        use_lowpass_filter: bool,
        zscore_threshold: float) -> pd.DataFrame:
    """
    Parameters
    ----------
    sync_file: str
        The path to the sync file
    behavior_stimulus_file: str
        path to behavior pickle file
    mapping_stimulus_file: str
        path to mapping pickle file
    replay_stimulus_file: str
        path to replay pickle file
    use_lowpass_filter: bool
        whther or not to apply a low pass filter to the
        running speed results
    zscore_threshold: float
        The threshold to use for removing outlier
        running speeds which might be noise and not true signal

    Returns
    -------
    A dataframe with columns 'timestamps' and 'speed'
    """

    (velocity_data,
     _) = multi_stim_running_df_from_raw_data(
             sync_path=sync_file,
             behavior_pkl_path=behavior_stimulus_file,
             mapping_pkl_path=mapping_stimulus_file,
             replay_pkl_path=replay_stimulus_file,
             use_lowpass_filter=use_lowpass_filter,
             zscore_threshold=zscore_threshold,
             behavior_start_frame=0)
    return pd.DataFrame(data={'timestamps': velocity_data.frame_time.values,
                              'speed': velocity_data.velocity.values})


class VBNRunningSpeed(RunningSpeedNWBMixin,
                      DataObject, LimsReadableInterface, NwbReadableInterface,
                      NwbWritableInterface, JsonReadableInterface,
                      JsonWritableInterface):
    """A DataObject which contains properties and methods to load, process,
    and represent running speed data.

    Running speed data is represented as:

    Pandas Dataframe with the following columns:
        "timestamps": Timestamps (in s) for calculated speed values
        "speed": Computed running speed in cm/s

    The difference betwen this class and the RunningSpeed class implemented
    in behavior/data_objects/running_speed is that this object concatenates
    the running speed data from multiple stimulus pickle files
    """

    def __init__(
            self,
            running_speed: pd.DataFrame,
            sync_file: Optional[str] = None,
            behavior_stimulus_file: Optional[str] = None,
            replay_stimulus_file: Optional[str] = None,
            mapping_stimulus_file: Optional[str] = None,
            use_lowpass_filter: bool = True,
            zscore_threshold: float = 10.0,
            filtered: bool = False):

        super().__init__(name='running_speed', value=running_speed)
        self._sync_file = sync_file
        self._behavior_stimulus_file = behavior_stimulus_file
        self._replay_stimulus_file = replay_stimulus_file
        self._mapping_stimulus_file = mapping_stimulus_file
        self._use_lowpass_filter = use_lowpass_filter
        self._zscore_threshold = zscore_threshold
        self._filtered = filtered

    @classmethod
    def from_json(
            cls,
            dict_repr: dict) -> "VBNRunningSpeed":

        behavior_stimulus_file = dict_repr['behavior_stimulus_file']
        replay_stimulus_file = dict_repr['replay_stimulus_file']
        mapping_stimulus_file = dict_repr['mapping_stimulus_file']
        sync_file = dict_repr['sync_file']
        if 'use_lowpass_filter' in dict_repr:
            use_lowpass_filter = dict_repr['use_lowpass_filter']
        else:
            use_lowpass_filter = True

        if 'zscore_threshold' in dict_repr:
            zscore_threshold = dict_repr['zscore_threshold']
        else:
            zscore_threshold = 10.0

        df = _get_multi_stim_running_df(
                sync_file=sync_file,
                behavior_stimulus_file=behavior_stimulus_file,
                replay_stimulus_file=replay_stimulus_file,
                mapping_stimulus_file=mapping_stimulus_file,
                use_lowpass_filter=use_lowpass_filter,
                zscore_threshold=zscore_threshold)

        return cls(
                running_speed=df,
                sync_file=sync_file,
                behavior_stimulus_file=behavior_stimulus_file,
                mapping_stimulus_file=mapping_stimulus_file,
                replay_stimulus_file=replay_stimulus_file,
                use_lowpass_filter=use_lowpass_filter,
                zscore_threshold=zscore_threshold)

    def to_json(self) -> dict:
        output = dict()
        msg = ""
        for value, key in zip((self._sync_file,
                               self._behavior_stimulus_file,
                               self._mapping_stimulus_file,
                               self._replay_stimulus_file),
                              ('sync_file',
                               'behavior_stimulus_file',
                               'mapping_stimulus_file',
                               'replay_stimulus_file')):
            if value is None:
                msg += (f"{key} is None; must be specified "
                        "for VBNRunningSpeed.to_json")

            output[key] = value

        if len(msg) > 0:
            raise RuntimeError(msg)

        output["use_lowpass_filter"] = self._use_lowpass_filter
        output["zscore_threshold"] = self._zscore_threshold
        output["filtered"] = self._filtered
        return output

    @classmethod
    def from_lims(cls):
        raise NotImplementedError()