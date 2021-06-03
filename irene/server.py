import requests
from typing import List, Optional, Dict, Any, Union
from .lang import QExpr
from dataclasses import dataclass, asdict


@dataclass
class DocResponse(object):
    name: str
    score: float
    document: Optional[Dict[str, Any]] = None  # if document asked for...
    rank: int = -1  # assigned on this side to save bandwidth/parsing time.

@dataclass
class SetResponse(object):
    matches: List[str]
    totalHits: int

@dataclass
class QueryResponse(object):
    topdocs: List[DocResponse]
    totalHits: int


@dataclass
class IndexInfo(object):
    defaultField: str
    path: str
    idFieldName: str


class IreneService(object):
    def __init__(self, host: str = "localhost", port: int = 1234):
        self.host = host
        self.port = port
        self.url = "http://{0}:{1}".format(host, port)

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
            raise ValueError("{}: {}".format(r.status_code, r.reason))
        return r.json()["terms"]

    def doc(self, index: str, name: str) -> Dict[str, Any]:
        params = {"id": name}
        r = requests.get(self._url("/api/doc/{}".format(index)), params)
        if not r.ok:
            raise ValueError("{}: {}".format(r.status_code, r.reason))
        return r.json()

    def random(self, index: str) -> str:
        r = requests.get(self._url("/api/random/{}".format(index)))
        if not r.ok:
            raise ValueError("{}: {}".format(r.status_code, r.reason))
        return r.json()["name"]

    def prepare(self, index: str, query: QExpr) -> Dict[str, Any]:
        params = {"index": index, "query": asdict(query)}
        r = requests.post(self._url("/api/prepare"), json=params)
        if not r.ok:
            raise ValueError("{}: {}".format(r.status_code, r.reason))
        return r.json()

    def sample(
        self, index: str, query: Union[Dict, QExpr], n: int, seed: Optional[int] = None
    ) -> SetResponse:
        # allow for dict-repr on outside!
        if not isinstance(query, dict):
            query = asdict(query)
        # data class SampleRequest(val index: String, val count: Int, val query: QExpr, val seed: Long? = null)
        params = {"index": index, "count": n, "query": query}
        if seed is not None:
            params["seed"] = seed
        response = requests.post(self._url("/api/sample"), json=params)
        if response.ok:
            r_json = response.json()
            return SetResponse(r_json["matches"], r_json["totalHits"])
        raise ValueError(
            "{}: {} with query={}".format(response.status_code, response.reason, query)
        )

    def docset(
        self, index: str, query: Union[Dict, QExpr], depth: int = 50
    ) -> SetResponse:
        # allow for dict-repr on outside!
        if not isinstance(query, dict):
            query = asdict(query)

        # data class QueryRequest(val index: String, val depth: Int, val query: QExpr)
        params = {"index": index, "depth": depth, "query": query}
        response = requests.post(self._url("/api/docset"), json=params)
        if response.ok:
            r_json = response.json()
            return SetResponse(r_json["matches"], r_json["totalHits"])
        raise ValueError("{0}: {1}".format(response.status_code, response.reason))

    def query(
        self,
        index: str,
        query: Union[Dict, QExpr],
        depth: int = 50,
        offset: int = 0,
        get_documents: bool = False,
    ) -> QueryResponse:
        # allow for dict-repr on outside!
        if not isinstance(query, dict):
            query = asdict(query)

        # data class QueryRequest(val index: String, val depth: Int, val query: QExpr)
        params = {
            "index": index,
            "depth": depth,
            "query": query,
            "offset": offset,
            "getDocuments": get_documents,
        }
        response = requests.post(self._url("/api/query"), json=params)
        if response.ok:
            r_json = response.json()
            topdocs = r_json["topdocs"]
            return QueryResponse(
                [DocResponse(rank=offset + i + 1, **td) for (i, td) in enumerate(topdocs)],
                r_json["totalHits"],
            )
        raise ValueError("{0}: {1}".format(response.status_code, response.reason))

    def indexes(self) -> Dict[str, IndexInfo]:
        json = requests.get(self._url("/api/indexes")).json()
        return dict((k, IndexInfo(**v)) for (k, v) in json.items())

    def config(self, index) -> Dict[str, Any]:
        r = requests.get(self._url("/api/config/{}".format(index)))
        if not r.ok:
            raise ValueError("{}: {}".format(r.status_code, r.reason))
        return r.json()


@dataclass
class IreneIndex(object):
    service: IreneService
    index: str

    def tokenize(self, text: str, field: Optional[str] = None) -> List[str]:
        return self.service.tokenize(self.index, text, field)

    def doc(self, name: str) -> Dict[str, Any]:
        return self.service.doc(self.index, name)

    def random(self) -> str:
        return self.service.random(self.index)

    def sample(
        self, query: Union[Dict, QExpr], n: int, seed: Optional[int] = None
    ) -> SetResponse:
        return self.service.sample(self.index, query, n, seed)

    def docset(self, query: Union[Dict, QExpr], n: int) -> SetResponse:
        return self.service.docset(self.index, query, n)

    def query(
        self,
        query: Union[Dict, QExpr],
        depth: int = 50,
        offset: int = 0,
        get_documents: bool = False,
    ) -> QueryResponse:
        return self.service.query(self.index, query, depth, offset, get_documents)

    def config(self) -> Dict[str, Any]:
        return self.service.config(self.index)

    def prepare(self, query: QExpr) -> Dict[str, Any]:
        return self.service.prepare(self.index, query)
