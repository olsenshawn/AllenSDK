import logging
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import pynwb
from pynwb import NWBFile

from allensdk.brain_observatory.ecephys._probe import Probe
from allensdk.brain_observatory.ecephys.nwb_util import \
    add_ragged_data_to_dynamic_table
from allensdk.core import DataObject, JsonReadableInterface, \
    NwbReadableInterface, NwbWritableInterface


def _get_structure_key(channels: pd.DataFrame) -> str:
    """
    Scan a channels dataframe to determine if the structure
    column is 'ecephys_structure_id' or 'manual_structure_id',

    Return the appropriate key.
    """
    candidate_list = ('ecephys_structure_id',
                      'manual_structure_id')
    for candidate in candidate_list:
        if candidate in channels.columns:
            return candidate
    msg = (f"Could not find {candidate_list} in channels data frame. "
           f"Columns present are {channels.columns}")
    raise RuntimeError(msg)


class Probes(DataObject, JsonReadableInterface, NwbReadableInterface,
             NwbWritableInterface):
    """Probes"""

    def __init__(self,
                 probes: List[Probe]):
        """

        Parameters
        ----------
        probes: List of Probe
        """
        self._probes = probes
        super().__init__(name='probes', value=self)

    @property
    def probes(self):
        return self._probes

    @property
    def spike_times(self) -> Dict[int, np.ndarray]:
        """

        Returns
        -------
        Dictionary mapping unit id to spike_times for all probes
        """
        return {
            unit.id: unit.spike_times
            for probe in self.probes
            for unit in probe.units.value}

    @property
    def mean_waveforms(self) -> Dict[int, np.ndarray]:
        """

        Returns
        -------
        Dictionary mapping unit id to mean_waveforms for all probes
        """
        return {
            unit.id: unit.mean_waveforms
            for probe in self.probes
            for unit in probe.units.value}

    @property
    def spike_amplitudes(self) -> Dict[int, np.ndarray]:
        """

        Returns
        -------
        Dictionary mapping unit id to spike_amplitudes for all probes
        """
        return {
            unit.id: unit.spike_amplitudes
            for probe in self.probes
            for unit in probe.units.value}

    def get_units_table(
            self,
            filter_by_validity: bool = True,
            filter_out_of_brain_units: bool = True,
            amplitude_cutoff_maximum: Optional[float] = None,
            presence_ratio_minimum: Optional[float] = None,
            isi_violations_maximum: Optional[float] = None
    ) -> pd.DataFrame:
        """
        Gets a dataframe representing all units detected by all probes

        Parameters
        ----------
        filter_by_validity
            Whether to filter out units without quality == "good"
        filter_out_of_brain_units
            Whether to filter out units with missing ecephys_structure_id
        amplitude_cutoff_maximum
            Filter units by this upper bound
        presence_ratio_minimum
            Filter units by this lower bound
        isi_violations_maximum
            Filter units by this upper bound
        Returns
        -------
        Dataframe containing all units detected by probes
        Columns:
            - properties of `allensdk.ecephys._unit.Unit`
            except for 'spike_times', 'spike_amplitudes', 'mean_waveforms'
            which are returned separately
        """

        units_table = pd.concat([probe.units_table for probe in self.probes])
        units_table = units_table.set_index(keys='id', drop=True)
        units_table = units_table.drop(columns=[
            'spike_times', 'spike_amplitudes', 'mean_waveforms'])
        if filter_by_validity or filter_out_of_brain_units:
            channels = pd.concat([
                p.channels.to_dataframe(
                    filter_by_validity=filter_by_validity
                ) for p in self.probes
            ])

            structure_key = _get_structure_key(channels)

            if filter_out_of_brain_units:
                channels = channels[~(channels[structure_key].isna())]

            # noinspection PyTypeChecker
            channel_ids = set(channels.index.values.tolist())
            units_table = units_table[
                units_table["peak_channel_id"].isin(channel_ids)]

        if filter_by_validity:
            units_table = units_table[units_table["quality"] == "good"]
            units_table.drop(columns=["quality"], inplace=True)

        units_table = units_table[
            units_table["amplitude_cutoff"] <=
            (amplitude_cutoff_maximum or np.inf)]
        units_table = units_table[
            units_table["presence_ratio"] >=
            (presence_ratio_minimum or -np.inf)]
        units_table = units_table[
            units_table["isi_violations"] <=
            (isi_violations_maximum or np.inf)]

        return units_table

    @classmethod
    def from_json(
            cls,
            probes: List[Dict[str, Any]],
            skip_probes: Optional[List[str]] = None
    ) -> "Probes":
        """

        Parameters
        ----------
        probes
        skip_probes: Names of probes to exclude (due to known bad data
            for example)
        Returns
        -------
        `Probes` instance
        """
        skip_probes = skip_probes if skip_probes is not None else []
        invalid_skip_probes = set(skip_probes).difference(
            [p['name'] for p in probes])
        if invalid_skip_probes:
            raise ValueError(
                f'You passed invalid probes to skip: {invalid_skip_probes} '
                f'are not valid probe names')
        for probe in skip_probes:
            logging.info(f'Skipping {probe}')
        probes = [p for p in probes if p['name'] not in skip_probes]
        probes = sorted(probes, key=lambda probe: probe['name'])
        probes = [Probe.from_json(probe=probe) for probe in probes]
        return Probes(probes=probes)

    def to_dataframe(self):
        probes = [probe.to_dict() for probe in self.probes]
        probes = pd.DataFrame(probes)
        probes = probes.set_index(keys='id')
        return probes

    @classmethod
    def from_nwb(cls, nwbfile: NWBFile) -> "Probes":
        probes = [
            Probe.from_nwb(
                nwbfile=nwbfile,
                probe_name=probe_name)
            for probe_name in nwbfile.electrode_groups]
        return Probes(probes=probes)

    def to_nwb(self, nwbfile: NWBFile) -> NWBFile:
        for probe in self.probes:
            probe.to_nwb(nwbfile=nwbfile)

        nwbfile.units = pynwb.misc.Units.from_dataframe(
            self.get_units_table(
                filter_by_validity=False,
                filter_out_of_brain_units=False),
            name='units')

        add_ragged_data_to_dynamic_table(
            table=nwbfile.units,
            data=self.spike_times,
            column_name="spike_times",
            column_description="times (s) of detected spiking events",
        )

        add_ragged_data_to_dynamic_table(
            table=nwbfile.units,
            data=self.spike_amplitudes,
            column_name="spike_amplitudes",
            column_description="amplitude (s) of detected spiking events"
        )

        add_ragged_data_to_dynamic_table(
            table=nwbfile.units,
            data=self.mean_waveforms,
            column_name="waveform_mean",
            column_description="mean waveforms on peak channels (over "
                               "samples)",
        )

        return nwbfile

    def __iter__(self):
        for p in self.probes:
            yield p