import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pynwb
import pytest
from pynwb import NWBFile

from allensdk.brain_observatory.behavior.data_files import BehaviorStimulusFile
from allensdk.brain_observatory.behavior.data_objects import StimulusTimestamps
from allensdk.brain_observatory.behavior.data_objects.stimuli.presentations \
    import \
    Presentations as StimulusPresentations, Presentations
from allensdk.brain_observatory.behavior.data_objects.stimuli.stimuli import \
    Stimuli
from allensdk.brain_observatory.behavior.data_objects.stimuli.templates \
    import \
    Templates
from allensdk.test.brain_observatory.behavior.data_objects.lims_util import \
    LimsTest


class TestFromBehaviorStimulusFile(LimsTest):
    @classmethod
    def setup_class(cls):
        cls.behavior_session_id = 994174745

        dir = Path(__file__).parent.resolve()
        test_data_dir = dir / 'test_data'

        presentations = \
            pd.read_pickle(str(test_data_dir / 'presentations.pkl'))
        templates = \
            pd.read_pickle(str(test_data_dir / 'templates.pkl'))
        cls.expected_presentations = StimulusPresentations(
            presentations=presentations)
        cls.expected_templates = Templates(templates=templates)

    @pytest.mark.requires_bamboo
    def test_from_stimulus_file(self):
        stimulus_file = BehaviorStimulusFile.from_lims(
            behavior_session_id=self.behavior_session_id, db=self.dbconn)
        stimulus_timestamps = StimulusTimestamps.from_stimulus_file(
            stimulus_file=stimulus_file,
            monitor_delay=0.0)
        stimuli = Stimuli.from_stimulus_file(
            stimulus_file=stimulus_file,
            stimulus_timestamps=stimulus_timestamps,
            limit_to_images=['im065'])
        assert stimuli.presentations == self.expected_presentations
        assert stimuli.templates == self.expected_templates


class TestPresentations:
    @classmethod
    def setup_class(cls):
        with open('/allen/aibs/informatics/module_test_data/ecephys/'
                  'ecephys_session_1111216934_input.json') \
                as f:
            cls.input_data = json.load(f)['session_data']
        cls._table_from_json = Presentations.from_path(
            path=cls.input_data['stim_table_file'])

    def setup_method(self, method):
        self._nwbfile = NWBFile(
            session_description='foo',
            identifier='foo',
            session_id='foo',
            session_start_time=datetime.now(),
            institution="Allen Institute"
        )
        # Need to write stimulus timestamps first
        bsf = BehaviorStimulusFile.from_json(dict_repr=self.input_data)
        ts = StimulusTimestamps.from_stimulus_file(stimulus_file=bsf,
                                                   monitor_delay=0.0)
        ts.to_nwb(nwbfile=self._nwbfile)

    @pytest.mark.requires_bamboo
    @pytest.mark.parametrize('roundtrip, add_is_change',
                             ([True, False], [True, False]))
    def test_read_write_nwb(self, roundtrip, add_is_change,
                            data_object_roundtrip_fixture):
        self._table_from_json.to_nwb(nwbfile=self._nwbfile)

        if roundtrip:
            obt = data_object_roundtrip_fixture(
                nwbfile=self._nwbfile,
                data_object_cls=Presentations,
                add_is_change=add_is_change
            )
        else:
            obt = Presentations.from_nwb(nwbfile=self._nwbfile,
                                         add_is_change=add_is_change)

        assert obt == self._table_from_json


class TestNWB:
    @classmethod
    def setup_class(cls):
        dir = Path(__file__).parent.resolve()
        cls.test_data_dir = dir / 'test_data'

        presentations = \
            pd.read_pickle(str(cls.test_data_dir / 'presentations.pkl'))
        templates = \
            pd.read_pickle(str(cls.test_data_dir / 'templates.pkl'))
        presentations = presentations.drop('is_change', axis=1)
        p = StimulusPresentations(presentations=presentations)
        t = Templates(templates=templates)
        cls.stimuli = Stimuli(presentations=p, templates=t)

    def setup_method(self, method):
        self.nwbfile = pynwb.NWBFile(
            session_description='asession',
            identifier='1234',
            session_start_time=datetime.now()
        )

        # Need to write stimulus timestamps first
        bsf = BehaviorStimulusFile(
            filepath=self.test_data_dir / 'behavior_stimulus_file.pkl')
        ts = StimulusTimestamps.from_stimulus_file(stimulus_file=bsf,
                                                   monitor_delay=0.0)
        ts.to_nwb(nwbfile=self.nwbfile)

    @pytest.mark.parametrize('roundtrip', [True, False])
    def test_read_write_nwb(self, roundtrip,
                            data_object_roundtrip_fixture):
        self.stimuli.to_nwb(nwbfile=self.nwbfile)

        if roundtrip:
            obt = data_object_roundtrip_fixture(
                nwbfile=self.nwbfile,
                data_object_cls=Stimuli)
        else:
            obt = Stimuli.from_nwb(nwbfile=self.nwbfile)

        # is_change different due to limit_to_images
        obt.presentations.value.drop('is_change', axis=1, inplace=True)

        assert obt == self.stimuli


@pytest.mark.parametrize("stimulus_table, expected_table_data", [
    ({'image_index': [8, 9],
      'image_name': ['omitted', 'not_omitted'],
      'image_set': ['omitted', 'not_omitted'],
      'index': [201, 202],
      'omitted': [True, False],
      'start_frame': [231060, 232340],
      'start_time': [0, 250],
      'stop_time': [None, 1340509],
      'duration': [None, 1340259]},
     {'image_index': [8, 9],
      'image_name': ['omitted', 'not_omitted'],
      'image_set': ['omitted', 'not_omitted'],
      'index': [201, 202],
      'omitted': [True, False],
      'start_frame': [231060, 232340],
      'start_time': [0, 250],
      'stop_time': [0.25, 1340509],
      'duration': [0.25, 1340259]}
     )
])
def test_set_omitted_stop_time(stimulus_table, expected_table_data):
    stimulus_table = pd.DataFrame.from_dict(data=stimulus_table)
    expected_table = pd.DataFrame.from_dict(data=expected_table_data)
    stimulus_table = \
        StimulusPresentations._fill_missing_values_for_omitted_flashes(
            df=stimulus_table)
    assert stimulus_table.equals(expected_table)


class TestTemplates:
    @classmethod
    def setup_class(cls):
        with open('/allen/aibs/informatics/module_test_data/ecephys/'
                  'ecephys_session_1111216934_input.json') \
                as f:
            cls.input_data = json.load(f)['session_data']
        sf = BehaviorStimulusFile.from_json(
            dict_repr=cls.input_data)
        cls._presentations_from_json = Presentations.from_path(
            path=cls.input_data['stim_table_file'])
        cls._templates_from_stim = \
            Templates.from_stimulus_file(stimulus_file=sf)

    def setup_method(self, method):
        self._nwbfile = NWBFile(
            session_description='foo',
            identifier='foo',
            session_id='foo',
            session_start_time=datetime.now(),
            institution="Allen Institute"
        )

    @pytest.mark.requires_bamboo
    @pytest.mark.parametrize('roundtrip', [True, False])
    def test_read_write_nwb_no_image_index(
            self, roundtrip, data_object_roundtrip_fixture):
        """This presentations table has no image_index.
        Make sure the roundtrip doesn't break"""
        self._templates_from_stim.to_nwb(
            nwbfile=self._nwbfile,
            stimulus_presentations=self._presentations_from_json)

        if roundtrip:
            obt = data_object_roundtrip_fixture(
                nwbfile=self._nwbfile,
                data_object_cls=Templates
            )
        else:
            obt = Templates.from_nwb(nwbfile=self._nwbfile)

        assert obt == self._templates_from_stim
