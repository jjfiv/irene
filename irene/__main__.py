from .server import IreneService
from .lang import *
import json
import attr
import sys

service = IreneService(port=4444)
print(service.indexes())

names = ['wiki']
if len(sys.argv) > 1:
    names = sys.argv[1:]

for name in names:
    index = service.index(name)
    terms = index.tokenize("hello world!")
    ql = CombineExpr(
            children=[DirQLExpr(TextExpr(t)) for t in terms], weights=[1.0 for t in terms]
    )
    qres = index.query(ql, 4)
    print("({})\tHello World: {} hits.".format(name, qres.totalHits))
    doc_name = index.random()
    print("({})\t{}".format(name, index.doc(doc_name)["body"][:100].replace('\n', ' ')))
