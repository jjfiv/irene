import requests
from typing import List, Optional, Dict, Any
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
        self.url = 'http://{0}:{1}'.format(host, port)
        self.known_open_indexes = {}
    
    def _url(self, path):
        return self.url + path
    
    def open(self, name: str, path: str) -> 'IreneIndex':
        available = self.indexes()
        if name in available and available[name].path == path:
            return IreneIndex(self, name)
        response = requests.post(self._url("/open"), data={'name': name, 'path': path})
        if response.ok:
            self.known_open_indexes[name] = path
            return IreneIndex(self, name)
        raise ValueError('{0}: {1}'.format(response.reason))
    
    def tokenize(self, index: str, text: str, field: Optional[str] = None) -> List[str]:
        params = {'index': index, 'text': text}
        if field is not None:
            params['field'] = field
        return requests.get(self._url("/tokenize"), params).json()['terms']
    
    def doc(self, index: str, name: str) -> Dict[str, Any]:
        params = {'index': index, 'id': name }
        return requests.get(self._url('/doc'), params).json()

    def query(self, index: str, query: QExpr, depth: int=50) -> QueryResponse:
       # data class QueryRequest(val index: String, val depth: Int, val query: QExpr)
       params = {'index': index, 'depth': depth, 'query': attr.asdict(query) } 
       print(json.dumps(params['query'], indent=2))
       r_json = requests.post(self._url('/query'), json=params).json()
       topdocs = r_json['topdocs']
       return QueryResponse([DocResponse(**td) for td in topdocs], r_json['totalHits'])

    def indexes(self) -> Dict[str,IndexInfo]:
        json = requests.get(self._url("/indexes")).json()
        return dict((k, IndexInfo(**v)) for (k,v) in json.items())
    
    def config(self, index) -> Dict[str, Any]:
       return requests.get(self._url("/config"), params={'index': index}).json()

@attr.s
class IreneIndex(object):
    service = attr.ib(type=IreneService)
    index = attr.ib(type=str)

    def tokenize(self, text: str, field: Optional[str] = None) -> List[str]:
        return self.service.tokenize(self.index, text, field)
    
    def doc(self, name: str) -> Dict[str, Any]:
        return self.service.doc(self.index, name)

    def query(self, query: QExpr, depth: int=50) -> QueryResponse:
        return self.service.query(self.index, query, depth)

    def config(self) -> Dict[str, Any]:
        return self.service.config(self.index)