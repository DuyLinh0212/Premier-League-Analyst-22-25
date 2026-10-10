"""Package this delivery root with checksums; never create delivery/delivery."""
import hashlib
import json
import os
import zipfile
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
EXCLUDE = {".git","node_modules",".next",".venv","__pycache__","artifacts","pipeline_runs","wheels","evidence","evidence-local","versions","hadoop"}
FOLDERS = ["analytics","data","jobs","tests","validation","scripts","Generate_LeagueTable_Api","FBref_Data","Transfermark_Data","portable","infra","Premier Leauge Club Analyst/web-next",".github"]

def included():
    for folder in FOLDERS:
        for current, dirs, names in os.walk(ROOT/folder):
            dirs[:] = [d for d in dirs if d not in EXCLUDE]
            for name in names:
                path = Path(current)/name
                if path.stat().st_size > 95 * 1024 * 1024 or name == ".env" or name.startswith(".env.") and name != ".env.example" or path.suffix in {".pyc",".log",".zip",".tar"}:
                    continue
                yield path
    for name in ["README.md","LICENSE",".dockerignore",".gitignore",".gitattributes"]:
        if (ROOT/name).exists():
            yield ROOT/name

def main():
    destination = ROOT/"artifacts"
    destination.mkdir(exist_ok=True)
    archive = destination/"PremierLeague_Docker.zip"
    staging = archive.with_suffix(".zip.part")
    manifest = {"format":2,"platform":"linux/amd64","deployment":"external infrastructure","files":[]}
    with zipfile.ZipFile(staging,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=6) as output:
        for path in sorted(set(included())):
            relative = path.relative_to(ROOT).as_posix()
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            output.write(path,relative)
            manifest["files"].append({"path":relative,"bytes":path.stat().st_size,"sha256":sha})
        output.writestr("bundle_manifest.json",json.dumps(manifest,ensure_ascii=False,indent=2))
    staging.replace(archive)
    (destination/"bundle_manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
    (destination/(archive.name+".sha256")).write_text(hashlib.sha256(archive.read_bytes()).hexdigest()+"  "+archive.name+"\n",encoding="utf-8")
    print(json.dumps({"archive":str(archive),"bytes":archive.stat().st_size,"files":len(manifest["files"])}))

if __name__ == "__main__":
    main()
