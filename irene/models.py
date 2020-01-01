from irene.lang import QExpr, MeanExpr, SumExpr, DirQLExpr, TextExpr, BM25Expr
from typing import List

def QueryLikelihood(words: List[str], scorer: lambda x: DirQLExpr(x)) -> QExpr:
    return MeanExpr([scorer(TextExpr(w)) for w in words])

def BM25(words: List[str]) -> QExpr:
    return SumExpr([BM25Expr(TextExpr(w)) for w in words])

def SequentialDependenceModel(words: List[str]):
    pass