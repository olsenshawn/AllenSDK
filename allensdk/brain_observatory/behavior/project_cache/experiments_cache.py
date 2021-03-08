from typing import Optional, List

import pandas as pd

from allensdk.brain_observatory.behavior.metadata.behavior_ophys_metadata \
    import \
    BehaviorOphysMetadata
from allensdk.brain_observatory.behavior.project_cache.cache_table import \
    CacheTable


class BehaviorExperimentsCacheTable(CacheTable):
    def __init__(self, df: pd.DataFrame,
                 suppress: Optional[List[str]] = None):
        super().__init__(df=df, suppress=suppress)

    def postprocess_additional(self):
        self._df['indicator'] = self._df['reporter_line'].apply(
            BehaviorOphysMetadata.parse_indicator)
