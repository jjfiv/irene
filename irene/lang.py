from dataclasses import dataclass, field, asdict
from typing import List, Optional


@dataclass
class CountStats:
    source: str
    cf: int
    df: int
    cl: int
    dc: int

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


class QExpr(object):
    def weighted(self, weight):
        return WeightExpr(child=self, weight=weight)


###
# Boolean opeartions
###

# Sync this class to Galago semantics.
#  - Consider every doc that has a match IFF cond has a match, using value, regardless of whether value also has a match.
#  - Implemented by [RequireEval].
#  - If instead you want to score value only if cond has a match, use [MustExpr] -> [MustEval].
@dataclass
class RequireExpr(QExpr):
    cond: QExpr
    value: QExpr
    kind: str = "Require"


# Score the [value] query when it matches IFF [must] also has a match. This is a logical AND.
@dataclass
class MustExpr(QExpr):
    must: QExpr
    value: QExpr
    kind: str = "Must"


@dataclass
class AndExpr(QExpr):
    children: List[QExpr]
    kind: str = "And"


@dataclass
class OrExpr(QExpr):
    children: List[QExpr]
    kind: str = "Or"


# AKA: True
class AlwaysMatchLeaf(QExpr):
    pass


# AKA: False
class NeverMatchLeaf(QExpr):
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


@dataclass
class CombineExpr(QExpr):
    children: List[QExpr]
    weights: List[float]
    kind: str = "Combine"


@dataclass
class MultExpr(QExpr):
    children: List[QExpr]
    kind: str = "Mult"


@dataclass
class MaxExpr(QExpr):
    children: List[QExpr]
    kind: str = "Max"


@dataclass
class WeightExpr(QExpr):
    child: QExpr
    weight: float
    kind: str = "Weight"


###
# Leaf Nodes
###


@dataclass
class TextExpr(QExpr):
    text: str
    field: Optional[str] = None
    stats_field: Optional[str] = None
    needed: Optional[str] = None
    kind: str = "Text"


def BoolExpr(field: str, desired: bool = True):
    return TextExpr(text="T" if desired else "F", field=field)


@dataclass
class LengthsExpr(QExpr):
    field: str


@dataclass
class WhitelistMatchExpr(QExpr):
    doc_names: List[str]


@dataclass
class DenseLongField(QExpr):
    name: str
    missing: int = 0


class DenseFloatField(QExpr):
    name: str
    missing: Optional[float] = None


###
# Phrase Nodes
###


@dataclass
class OrderedWindowExpr(QExpr):
    children: List[QExpr]
    step: int = 1
    kind: str = "OrderedWindow"


@dataclass
class UnorderedWindowNode(QExpr):
    children: List[QExpr]
    width: int = 8
    kind: str = "UnorderedWindow"


@dataclass
class SmallestCountExpr(QExpr):
    children: List[QExpr]


@dataclass
class SynonymExpr(QExpr):
    children: List[QExpr]
    kind: str = "Synonym"


###
# Scorers
###


@dataclass
class BM25Expr(QExpr):
    child: QExpr
    b: Optional[float] = None
    k: Optional[float] = None
    stats: Optional[CountStats] = None
    kind: str = "BM25"


@dataclass
class LinearQLExpr(QExpr):
    child: QExpr
    lambda_: Optional[float] = None
    stats: Optional[CountStats] = None
    kind: str = "LinearQL"


@dataclass
class DirQLExpr(QExpr):
    child: QExpr
    mu: Optional[float] = None
    stats: Optional[CountStats] = None
    kind: str = "DirQL"


@dataclass
class RM3Expr(QExpr):
    child: QExpr
    orig_weight: float = 0.3
    fb_docs: int = 20
    fb_terms: int = 100
    stopwords: bool = True
    field: Optional[str] = None
    kind: str = "RM3"


if __name__ == "__main__":
    expr = WeightExpr(child=TextExpr("hello"), weight=0.5)
    print(expr)
#%%
