#%%
from abc import abstractmethod, ABC
import attr
from typing import List, Optional

@attr.s
class CountStats(object):
    source = attr.ib(type=str)
    cf = attr.ib(type=int)
    df = attr.ib(type=int)
    cl = attr.ib(type=int)
    dc = attr.ib(type=int)
    
    def average_doc_len(self):
        if self.dc == 0:
            return 0.0
        return self.cl / self.dc
    def count_probability(self):
        if self.cl == 0:
            return 0.0
        return self.cf / self.cl
    def nonzero_count_probability(self):
        if self.cl == 0:
            return 0.0
        return max(0.5, self.cf) / self.cl
    def binary_probability(self):
        if self.dc == 0:
            return 0.0
        return self.df / self.dc

@attr.s
class QExpr(object):
    def children(self):
        return list(attr.asdict(self, recurse=False, filter=lambda attr,x: isinstance(x, QExpr)).values())
    def weighted(self, weight):
        return WeightExpr(child=self, weight=weight)

###
# Boolean opeartions
###

# Sync this class to Galago semantics.
#  - Consider every doc that has a match IFF cond has a match, using value, regardless of whether value also has a match.
#  - Implemented by [RequireEval].
#  - If instead you want to score value only if cond has a match, use [MustExpr] -> [MustEval].
@attr.s
class RequireExpr(object):
    cond = attr.ib(type=QExpr)
    value = attr.ib(type=QExpr)
    kind = attr.ib(type=str, default='Require')

# Score the [value] query when it matches IFF [must] also has a match. This is a logical AND.
@attr.s
class MustExpr(object):
    cond = attr.ib(type=QExpr)
    value = attr.ib(type=QExpr)

@attr.s
class AndExpr(QExpr):
    children: List[QExpr] = attr.ib()

@attr.s
class OrExpr(QExpr):
    children: List[QExpr] = attr.ib()

# AKA: True
@attr.s
class AlwaysMatchLeaf(object):
    pass

# AKA: False
@attr.s
class NeverMatchLeaf(object):
    pass

###
# Scoring Transformations
###

def SumExpr(children: List[QExpr]):
    N = len(children)
    return CombineExpr(children, [1.0 for _ in children])

def MeanExpr(children: List[QExpr]):
    N = len(children)
    return CombineExpr(children, [1.0 / N for _ in children])

@attr.s
class CombineExpr(QExpr):
    children: List[QExpr] = attr.ib()
    weights: List[float] = attr.ib()
    kind = attr.ib(type=str, default='Combine')

@attr.s
class MultExpr(QExpr):
    children: List[QExpr] = attr.ib()
    kind = attr.ib(type=str, default='Mult')

@attr.s
class MaxExpr(QExpr):
    children: List[QExpr] = attr.ib()
    kind = attr.ib(type=str, default='Max')

@attr.s
class WeightExpr(QExpr):
    child = attr.ib(type=QExpr)
    weight = attr.ib(type=float)
    kind = attr.ib(type=str, default='Weight')

###
# Leaf Nodes
###

@attr.s
class TextExpr(QExpr):
    text = attr.ib(type=str)
    field = attr.ib(type=Optional[str], default=None)
    stats_field = attr.ib(type=Optional[str], default=None)
    kind = attr.ib(type=str, default='Text')

@attr.s
class BoolExpr(QExpr):
    field = attr.ib(type=str)
    desired = attr.ib(type=bool, default=True)

@attr.s
class LengthsExpr(QExpr):
    field = attr.ib(type=str)

@attr.s
class WhitelistMatchExpr(QExpr):
    doc_names: List[str] = attr.ib()

@attr.s
class DenseLongField(QExpr):
    name = attr.ib(type=str)
    missing = attr.ib(type=int, default=0)

class DenseFloatField(QExpr):
    name = attr.ib(type=str)
    # TODO: how do I float32::min in python?
    missing = attr.ib(type=float, default=None)

###
# Phrase Nodes
###

@attr.s
class OrderedWindowExpr(QExpr):
    children: List[QExpr] = attr.ib()
    step = attr.ib(type=int, default=1)
    kind = attr.ib(type=str, default="OrderedWindow")

@attr.s
class UnorderedWindowNode(QExpr):
    children: List[QExpr] = attr.ib()
    width = attr.ib(type=int, default=8)
    kind = attr.ib(type=str, default="UnorderedWindow")

@attr.s
class SmallestCountExpr(QExpr):
    children: List[QExpr] = attr.ib()

@attr.s
class SynonymExpr(QExpr):
    children: List[QExpr] = attr.ib()
    kind = attr.ib(type=str, default="Synonym")

###
# Scorers
###

@attr.s
class BM25Expr(QExpr):
    child = attr.ib(type=QExpr)
    b = attr.ib(type=Optional[float], default=None)
    k = attr.ib(type=Optional[float], default=None)
    stats = attr.ib(type=CountStats, default=None)
    kind = attr.ib(type=str, default='BM25Expr')

@attr.s
class LinearQLExpr(QExpr):
    child = attr.ib(type=QExpr)
    lambda_ = attr.ib(type=Optional[float], default=None)
    stats = attr.ib(type=CountStats, default=None)
    kind = attr.ib(type=str, default='LinearQL')

@attr.s
class DirQLExpr(QExpr):
    child = attr.ib(type=QExpr)
    mu = attr.ib(type=Optional[float], default=None)
    stats = attr.ib(type=CountStats, default=None)
    kind = attr.ib(type=str, default='DirQL')

@attr.s
class RM3Expr(QExpr):
    child = attr.ib(type=QExpr)
    orig_weight = attr.ib(type=float, default=0.3)
    fb_docs = attr.ib(type=int, default=20)
    fb_terms = attr.ib(type=int, default=100)
    stopwords = attr.ib(type=bool, default=True)
    field = attr.ib(type=Optional[str], default=None)
    kind = attr.ib(type=str, default='RM3')

if __name__ == '__main__':
    expr = WeightExpr(child=TextExpr("hello"), weight=0.5)
    print(expr)
    print(expr.children())
#%%
