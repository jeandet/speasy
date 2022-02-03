from typing import Dict
from types import SimpleNamespace
from speasy.inventory.indexes import ParameterIndex, DatasetIndex, ComponentIndex
from speasy.inventory import flat_inventories


class CDAIndex:
    def __init__(self, id: str):
        self._id = id

    def dl_kw_args(self):
        return {'xmlid': self._id}


class CDAPathIndex(SimpleNamespace):
    def __init__(self, **meta):
        super().__init__(**meta)


class CDAComponentIndex(CDAIndex, ComponentIndex):  # lgtm [py/conflicting-attributes]
    def __init__(self, **meta):
        id = meta.pop('serviceprovider_ID')
        name = meta.pop('name')
        ComponentIndex.__init__(self=self, name=name, provider="cda", meta=meta)
        CDAIndex.__init__(self=self, id=id)
        flat_inventories.cda.components[id] = self


class CDAParameterIndex(CDAIndex, ParameterIndex):  # lgtm [py/conflicting-attributes]
    def __init__(self, **meta):
        id = meta.pop('serviceprovider_ID')
        name = meta.pop('name')
        ParameterIndex.__init__(self=self, name=name, provider="cda", meta=meta)
        CDAIndex.__init__(self=self, id=id)
        flat_inventories.cda.parameters[id] = self


class CDADatasetIndex(CDAIndex, DatasetIndex):  # lgtm [py/conflicting-attributes]

    def __init__(self, **meta):
        id = meta.pop('serviceprovider_ID')
        name = meta.pop('name')
        DatasetIndex.__init__(self=self, name=name, provider="cda", meta=meta)
        CDAIndex.__init__(self=self, id=id)
        flat_inventories.cda.datasets[id] = self
