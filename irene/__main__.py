from .server import IreneService
from .lang import *
import sys

service = IreneService(port=4444)
print(service.indexes())

names = ["wapo"]
if len(sys.argv) > 1:
    names = sys.argv[1:]

for name in names:
    index = service.index(name)
    terms = index.tokenize("hello world!")
    ql = SumExpr([DirQLExpr(TextExpr(t)) for t in terms])
    qres = index.query(ql, 4, get_documents=True)
    print("({})\tHello World: {} hits.".format(name, qres.totalHits))
    doc_name = index.random()
    print("({})\t{}".format(name, index.doc(doc_name)["body"][:100].replace("\n", " ")))
