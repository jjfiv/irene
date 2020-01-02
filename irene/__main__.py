from .server import IreneService
from .lang import *
import json
import attr

service = IreneService()
index = service.open('robust', 'robust04.irene')
print(service.indexes())
print(index.config())


terms = index.tokenize("hello world!")
ql = RM3Expr(
    CombineExpr(
        children=[DirQLExpr(TextExpr(t)) for t in terms], 
        weights=[1.0 for t in terms]))

print(index.query(ql, 20))
print(index.doc('LA081890-0076')['body'][:100])