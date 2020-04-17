#%%
from irene import IreneService, IreneIndex, QueryResponse, DocResponse
from irene.lang import *
import json
from tqdm import tqdm
from irene.models import QueryLikelihood

#%%
HOST = "localhost"
PORT = 1234
service = IreneService(HOST, PORT)
index = service.open("robust", "robust04.irene")

#%%
titles = {}
with open("queries/robust04/rob04.titles.tsv") as fp:
    for line in fp:
        [qid, text] = line.strip().split("\t")
        titles[qid] = text


# %%
queries = sorted(titles.keys())

# %%
sys_name = "robust_short_queries_roberta_base_only_widened"

variants = {}
with open("data/{0}.json".format(sys_name), "r") as fp:
    variants = json.load(fp)

#%%
orig_weight = 0.3
fb_docs = 20
fb_terms = 100

with open("data/{0}.trecrun".format(sys_name), "w") as out:
    for qid in tqdm(queries):
        [vtext, vweights] = variants[qid]
        opts = vtext.strip().split("\t")[:2]
        print(opts)
        var_q = MeanExpr(
            [
                RM3Expr(
                    QueryLikelihood(index.tokenize(opt)), orig_weight, fb_docs, fb_terms
                )
                for opt in opts
            ]
        )
        # full_query = index.prepare(var_q)
        results = index.query(var_q, depth=1000)
        for (i, doc) in enumerate(results.topdocs):
            rank = i + 1
            print(
                "{0} Q0 {1} {2} {3} {4}".format(
                    qid, doc.name, rank, doc.score, sys_name
                ),
                file=out,
            )


# %%
