import requests
from typing import List, Optional, Dict, Any, Union
from .lang import QExpr
import attr
import json


@attr.s
class DocResponse(object):
    name = attr.ib(type=str)
    score = attr.ib(type=float)


@attr.s
class QueryResponse(object):
    topdocs = attr.ib(type=List[DocResponse])
    totalHits = attr.ib(type=int)


@attr.s
class IndexInfo(object):
    defaultField = attr.ib(type=str)
    path = attr.ib(type=str)
    idFieldName = attr.ib(type=str)


#%%
class IreneService(object):
    def __init__(self, host="localhost", port=1234):
        self.host = host
        self.port = port
        self.url = "http://{0}:{1}".format(host, port)
        self.known_open_indexes = {}

    def _url(self, path):
        return self.url + path

    def index(self, name: str) -> "IreneIndex":
        available = self.indexes()
        if name in available:
            return IreneIndex(self, name)
        raise ValueError("Not found index='{}'".format(name))

    def tokenize(self, index: str, text: str, field: Optional[str] = None) -> List[str]:
        params = {"text": text}
        if field is not None:
            params["field"] = field
        r = requests.get(self._url("/api/tokenize/{}".format(index)), params)
        if not r.ok:
            raise ValueError('{}: {}'.format(r.status_code, r.reason)) 
        return r.json()["terms"]

    def doc(self, index: str, name: str) -> Dict[str, Any]:
        params = {"id": name}
        r = requests.get(self._url("/api/doc/{}".format(index)), params)
        if not r.ok:
            raise ValueError('{}: {}'.format(r.status_code, r.reason))
        return r.json()

    def random(self, index: str) -> str:
        r = requests.get(self._url("/api/random/{}".format(index)))
        if not r.ok:
            raise ValueError('{}: {}'.format(r.status_code, r.reason))
        return r.json()['name']

    def prepare(self, index: str, query: QExpr) -> Dict[str, Any]:
        params = {"index": index, "query": attr.asdict(query)}
        r = requests.post(self._url("/api/prepare"), json=params)
        if not r.ok:
            raise ValueError('{}: {}'.format(r.status_code, r.reason))
        return r.json()

    def query(
        self, index: str, query: Union[Dict, QExpr], depth: int = 50
    ) -> QueryResponse:
        # allow for dict-repr on outside!
        if not isinstance(query, dict):
            query = attr.asdict(query)

        # data class QueryRequest(val index: String, val depth: Int, val query: QExpr)
        params = {"index": index, "depth": depth, "query": query}
        response = requests.post(self._url("/api/query"), json=params)
        if response.ok:
            r_json = response.json()
            topdocs = r_json["topdocs"]
            return QueryResponse([DocResponse(**td) for td in topdocs], r_json["totalHits"])
        raise ValueError("{0}: {1}".format(response.status_code, response.reason))

    def indexes(self) -> Dict[str, IndexInfo]:
        json = requests.get(self._url("/api/indexes")).json()
        return dict((k, IndexInfo(**v)) for (k, v) in json.items())

    def config(self, index) -> Dict[str, Any]:
        return requests.get(self._url("/api/config/{}".format(index)))


@attr.s
class IreneIndex(object):
    service = attr.ib(type=IreneService)
    index = attr.ib(type=str)

    def tokenize(self, text: str, field: Optional[str] = None) -> List[str]:
        return self.service.tokenize(self.index, text, field)

    def doc(self, name: str) -> Dict[str, Any]:
        return self.service.doc(self.index, name)
    
    def random(self ) -> Dict[str, Any]:
        return self.service.random(self.index)

    def query(self, query: Union[Dict, QExpr], depth: int = 50) -> QueryResponse:
        return self.service.query(self.index, query, depth)

    def config(self) -> Dict[str, Any]:
        return self.service.config(self.index)

    def prepare(self, query: QExpr) -> Dict[str, Any]:
        return self.service.prepare(self.index, query)
