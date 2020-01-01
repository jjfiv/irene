from .server import IreneService
from .lang import *
import json
import attr

service = IreneService()
INDEX = 'robust'
service.open(INDEX, 'robust04.irene')
terms = service.tokenize(INDEX, "hello world!")
ql = RM3Expr(
    CombineExpr(
        children=[DirQLExpr(TextExpr(t)) for t in terms], 
        weights=[1.0 for t in terms]))

print(service.query(INDEX, ql, 20))
print(service.doc(INDEX, 'LA081890-0076'))