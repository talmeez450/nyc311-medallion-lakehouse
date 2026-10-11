import sys, ast, json, os, tempfile, shutil, re
sys.path.insert(0, "notebooks")
import _common as c
from _common import *

src = open("notebooks/01_api_to_landing.py").read()
tree = ast.parse(src)
fn = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "land_batch"][0]
code = ast.get_source_segment(src, fn)

class FS:
    def rm(self, p, r=False): shutil.rmtree(p, ignore_errors=True)
class DBU: fs = FS()
dbutils = DBU()

ALL = [{"unique_key": str(10000000 + i), "created_date": "2026-03-01T00:00:00.000"} for i in range(55)]
calls = []
class Resp:
    def __init__(s, rows): s.rows = rows; s.content = json.dumps(rows).encode()
    def json(s): return s.rows
def fake_api_get(params, token="", **kw):
    calls.append(params["$where"])
    m = re.search(r"unique_key > '(\d+)'", params["$where"])
    rows = [r for r in ALL if not m or r["unique_key"] > m.group(1)]
    return Resp(rows[: params["$limit"]])
api_get = fake_api_get
exec(code)

d = tempfile.mkdtemp()
m = land_batch("created_date >= 'x'", d + "/b1", 20, 0, "", {"batch_id": "b1"})
print({k: m[k] for k in ("pages", "rows", "truncated")})
files = sorted(os.listdir(os.path.join(d, "b1", f"extract_ts={m['extract_ts']}")))
print(files, os.path.exists(d + "/b1/_manifest.json"))
print("second call where:", calls[1])
# re-run replaces, not duplicates
m2 = land_batch("created_date >= 'x'", d + "/b1", 20, 0, "", {"batch_id": "b1"})
print("rerun folders:", [x for x in os.listdir(d + "/b1")])
# truncated test
m3 = land_batch("created_date >= 'x'", d + "/b2", 20, 1, "", {"batch_id": "b2"})
print("max_pages=1 ->", m3["pages"], m3["rows"], m3["truncated"])
# exact multiple edge case: 40 rows, page 20 -> third call returns empty -> 2 pages
ALL[:] = ALL[:40]
m4 = land_batch("w", d + "/b3", 20, 0, "", {"batch_id": "b3"})
print("exact-multiple ->", m4["pages"], m4["rows"])
