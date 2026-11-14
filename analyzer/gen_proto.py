"""Генерирует Python-код из proto/analyzer/v1/analyzer.proto в папку gen/.
Запускать из папки analyzer:  python gen_proto.py"""

from pathlib import Path

from grpc_tools import protoc

here = Path(__file__).resolve().parent
proto_dir = here.parent / "proto" / "analyzer" / "v1"
out = here / "gen"
out.mkdir(exist_ok=True)

code = protoc.main([
    "grpc_tools.protoc",
    f"-I{proto_dir}",
    f"--python_out={out}",
    f"--grpc_python_out={out}",
    str(proto_dir / "analyzer.proto"),
])
if code != 0:
    raise SystemExit(f"protoc failed with code {code}")
print(f"ok: {out}")
