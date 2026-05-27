#!/usr/bin/env python3
import os, sys, subprocess, tempfile, shutil
from pathlib import Path
import osmium

class Config:
    TILEMAKER_DIR = Path("/mnt/wd_2tb/mvt/tilemaker")
    OUTPUT_DIR = Path(__file__).parent / "pmtiles_omt"
    OSMAND_DIR = Path("/home/xmd5a/utilites/OsmAndMapCreator-main")
    PMTILES_TOOL = Path(__file__).parent / "pmtiles"
    DOCKER_IMAGE = "versatiles/versatiles-tilemaker"
    STORE_METHOD = "clustered"

Config.OUTPUT_DIR.mkdir(exist_ok=True, mode=0o775)

def get_bbox(pbf_path: Path) -> str:
    class BBoxHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.min_lon = self.min_lat = float('inf')
            self.max_lon = self.max_lat = float('-inf')
        def node(self, n):
            self.min_lon = min(self.min_lon, n.location.lon)
            self.max_lon = max(self.max_lon, n.location.lon)
            self.min_lat = min(self.min_lat, n.location.lat)
            self.max_lat = max(self.max_lat, n.location.lat)
    
    handler = BBoxHandler()
    handler.apply_file(str(pbf_path), locations=True, idx='flex_mem')
    
    if handler.min_lon == float('inf'):
        return None
    
    return f"{handler.min_lon:.6f},{handler.min_lat:.6f},{handler.max_lon:.6f},{handler.max_lat:.6f}"

def split_ways(input_pbf: Path, output_pbf: Path) -> bool:
    class MaxIdHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.max_way = self.max_rel = 0
        def way(self, w): 
            self.max_way = max(self.max_way, w.id)
        def relation(self, r): 
            self.max_rel = max(self.max_rel, r.id)

    class SplitHandler(osmium.SimpleHandler):
        def __init__(self, writer, max_way, max_rel):
            super().__init__()
            self.writer = writer
            self.next_way = max_way + 1
            self.next_rel = max_rel + 1
        
        def node(self, n): 
            self.writer.add_node(n)
        
        def way(self, w):
            nodes = list(w.nodes)
            if len(nodes) <= 2000:
                self.writer.add_way(w)
                return
            
            tags = dict(w.tags)
            closed = len(nodes) > 1 and nodes[0].ref == nodes[-1].ref
            
            if closed:
                refs = [n.ref for n in nodes[:-1]]
                segments = []
                for i in range(0, len(refs), 1999):
                    seg = refs[i:i+1999]
                    if len(seg) < 2: continue
                    if i + 1999 < len(refs):
                        seg.append(refs[i+1999])
                    else:
                        seg.append(refs[0])
                    
                    new = osmium.osm.mutable.Way()
                    new.id = self.next_way
                    new.nodes = seg
                    self.writer.add_way(new)
                    segments.append(self.next_way)
                    self.next_way += 1
                
                if segments:
                    rel = osmium.osm.mutable.Relation()
                    rel.id = self.next_rel
                    rel.tags = tags
                    rel.tags['type'] = 'multipolygon'
                    for s in segments:
                        rel.members.append(osmium.osm.RelationMember(s, 'w', 'outer'))
                    self.writer.add_relation(rel)
                    self.next_rel += 1
            else:
                for i in range(0, len(nodes)-1, 1998):
                    end = min(i+2000, len(nodes))
                    seg = [n.ref for n in nodes[i:end]]
                    if len(seg) < 2: continue
                    
                    new = osmium.osm.mutable.Way()
                    new.id = self.next_way
                    new.tags = tags
                    new.nodes = seg
                    self.writer.add_way(new)
                    self.next_way += 1
        
        def relation(self, r): 
            self.writer.add_relation(r)

    try:
        mh = MaxIdHandler()
        mh.apply_file(str(input_pbf))
        
        writer = osmium.SimpleWriter(str(output_pbf))
        SplitHandler(writer, mh.max_way, mh.max_rel).apply_file(str(input_pbf))
        writer.close()
        return True
    except Exception as e:
        print(f"Split error: {e}")
        return False

def process_file(input_path: Path) -> bool:
    if not input_path.exists():
        print(f"File not found: {input_path}")
        return False
    
    out_file = Config.OUTPUT_DIR / f"{input_path.stem}.pmtiles"
    print(f"Processing: {input_path} -> {out_file}")

    with tempfile.TemporaryDirectory(prefix=f"omt_{os.getpid()}_") as tmp_dir:
        tmp = Path(tmp_dir)
        current_file = input_path
        
        # Convert OBF to OSM if needed
        if input_path.suffix.lower() == '.obf':
            print("Converting OBF → OSM...")
            osm_file = tmp / f"{input_path.stem}.osm"
            
            if not (Config.OSMAND_DIR / "inspector.sh").exists():
                print("Error: OsmAndMapCreator not found")
                return False
            
            cmd = ["bash", str(Config.OSMAND_DIR / "inspector.sh"),
                   "-vmap", "-vmapobjects", "-zoom=17", "-osm", str(input_path)]
            
            with open(osm_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                      text=True, cwd=Config.OSMAND_DIR)
            
            if result.returncode != 0 or not osm_file.exists() or osm_file.stat().st_size == 0:
                print("Error: OBF conversion failed")
                return False
            
            print(f"✓ OBF converted, size: {osm_file.stat().st_size} bytes")
            current_file = osm_file
        
        # Convert to PBF if needed
        if current_file.suffix.lower() in ['.osm', '.xml']:
            print("Converting to PBF...")
            pbf_file = tmp / f"{current_file.stem}.pbf"
            result = subprocess.run(["osmium", "cat", "--overwrite", str(current_file), 
                                   "-o", str(pbf_file)], capture_output=True)
            if result.returncode != 0:
                print("Error: OSM to PBF conversion failed")
                return False
            current_file = pbf_file
        
        # Sort PBF
        print("Sorting PBF...")
        sorted_pbf = tmp / "sorted.pbf"
        result = subprocess.run(["osmium", "sort", "--overwrite", str(current_file), 
                               "-o", str(sorted_pbf)], capture_output=True)
        if result.returncode != 0:
            print("Error: PBF sorting failed")
            return False
        
        # Split long ways
        print("Splitting long ways...")
        split_pbf = tmp / "split.pbf"
        if not split_ways(sorted_pbf, split_pbf):
            print("Error: Way splitting failed")
            return False
        
        if not split_pbf.exists() or split_pbf.stat().st_size == 0:
            print("Error: Split PBF is empty")
            return False
        
        # Get bbox
        bbox = get_bbox(split_pbf)
        bbox_args = ["--bbox", bbox] if bbox else []
        print(f"BBOX: {bbox or 'full'}")
        
        # Prepare mount points
        mounts = [
            "-v", f"{Config.TILEMAKER_DIR}/resources:/config",
            "-v", f"{split_pbf.parent}:/input",
            "-v", f"{Config.OUTPUT_DIR}:/output",
            "-v", f"{tmp}:/tmp"
        ]
        
        if (Config.TILEMAKER_DIR / "coastline").exists():
            mounts.extend(["-v", f"{Config.TILEMAKER_DIR / 'coastline'}:/coastline"])
        if (Config.TILEMAKER_DIR / "landcover").exists():
            mounts.extend(["-v", f"{Config.TILEMAKER_DIR / 'landcover'}:/landcover"])
        
        # Run tilemaker
        print("Running tilemaker...")
        cmd = ["docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}"] + mounts + [
               "--entrypoint", "tilemaker", Config.DOCKER_IMAGE,
               "--config", "/config/config-openmaptiles.json",
               "--process", "/config/process-openmaptiles.lua",
               *bbox_args, "--store", f"/tmp/{Config.STORE_METHOD}",
               "--input", f"/input/{split_pbf.name}", 
               "--output", f"/output/{out_file.name}"]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Tilemaker error: {result.stderr}")
            return False
        
        if not out_file.exists():
            print("Error: PMTiles file not created")
            return False
        
        # Cluster
        if Config.PMTILES_TOOL.exists():
            print("Clustering PMTiles...")
            subprocess.run([str(Config.PMTILES_TOOL), "cluster", str(out_file)], 
                         capture_output=True)
        
        print(f"✓ Success: {out_file} ({out_file.stat().st_size} bytes)")
        return True

def main():
    if len(sys.argv) < 2:
        print("Usage: python prepare_pmtiles.py <file.osm/pbf/obf>...")
        sys.exit(1)
    
    success = 0
    for f in sys.argv[1:]:
        if process_file(Path(f)):
            success += 1
        else:
            print(f"✗ Failed: {f}")
        print("-" * 50)
    
    print(f"Result: {success}/{len(sys.argv)-1} successful")
    sys.exit(0 if success == len(sys.argv)-1 else 1)

if __name__ == "__main__":
    main()