"""
Enhanced Knowledge Graph Constructor - Quality and performance
"""
import json
from neo4j import GraphDatabase
from entity_extractor import load_nlp, extract_entities
from relationship_extractor import find_relationships

# Config
DATA_FILE = "/Users/kabir/Desktop/Research/Implementation/good_raw.jsonl"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j" 
NEO4J_PASSWORD = "password"
MAX_RECORDS = 1000  # Balanced for quality and speed

def load_data(max_records=None):
    """Load JSONL data efficiently"""
    data = []
    print(f"Loading up to {max_records} records...")
    
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if max_records and i >= max_records:
                break
            try:
                record = json.loads(line.strip())
                if 'text' in record and len(record['text']) > 100:  # Quality filter
                    data.append({
                        'text': record['text'],
                        'domain': record.get('domain', 'unknown'),
                        'title': record.get('title', 'unknown')
                    })
            except json.JSONDecodeError:
                continue
    
    print(f"Loaded {len(data)} quality records")
    return data

def write_to_neo4j(triples, metadata_list):
    """Enhanced Neo4j writing with metadata"""
    if not triples:
        return
        
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    with driver.session() as session:
        # Batch write for performance
        batch_size = 50
        for i in range(0, len(triples), batch_size):
            batch = triples[i:i+batch_size]
            
            def write_batch(tx):
                for j, (subj, rel, obj) in enumerate(batch):
                    metadata = metadata_list[i+j] if i+j < len(metadata_list) else {}
                    
                    # Enhanced query with metadata
                    query = """
                    MERGE (s:Entity {name: $subj})
                    SET s.type = COALESCE(s.type, $subj_type),
                        s.domain = COALESCE(s.domain, $domain)
                    MERGE (o:Entity {name: $obj}) 
                    SET o.type = COALESCE(o.type, $obj_type),
                        o.domain = COALESCE(o.domain, $domain)
                    MERGE (s)-[r:`""" + rel.replace(' ', '_') + """`]->(o)
                    SET r.weight = COALESCE(r.weight, 0) + 1,
                        r.source = $source
                    """
                    
                    tx.run(query, 
                          subj=subj, obj=obj,
                          subj_type='ENTITY', obj_type='ENTITY',
                          domain=metadata.get('domain', 'unknown'),
                          source=metadata.get('title', 'unknown'))
            
            session.execute_write(write_batch)
    
    # Create indices for performance
    with driver.session() as session:
        try:
            session.run("CREATE INDEX entity_name IF NOT EXISTS FOR (n:Entity) ON (n.name)")
            session.run("CREATE INDEX entity_domain IF NOT EXISTS FOR (n:Entity) ON (n.domain)")
        except:
            pass  # Indices might already exist
    
    driver.close()

def get_stats(triples):
    """Quick statistics"""
    if not triples:
        return "No relationships found"
    
    relations = {}
    for _, rel, _ in triples:
        relations[rel] = relations.get(rel, 0) + 1
    
    top_rels = sorted(relations.items(), key=lambda x: x[1], reverse=True)[:5]
    return f"Top relations: {top_rels}"

def process_data(texts, nlp):
    """Process texts efficiently"""
    all_triples = []
    all_metadata = []
    
    for i, item in enumerate(texts):
        if i % 50 == 0:
            print(f"  Processing {i}/{len(texts)}...")
        
        text = item['text']
        entities = extract_entities(text, nlp)
        
        if len(entities) >= 2:  # Need at least 2 entities for relationships
            triples = find_relationships(text, entities, nlp)
            for triple in triples:
                all_triples.append(triple)
                all_metadata.append(item)
    
    return all_triples, all_metadata

def main():
    print("🚀 Enhanced Knowledge Graph Pipeline")
    print("=" * 40)
    
    # Load spaCy
    print("Loading spaCy model...")
    nlp = load_nlp()
    
    # Load data
    texts = load_data(MAX_RECORDS)
    if not texts:
        print("No data to process!")
        return
    
    # Process
    print(f"Processing {len(texts)} texts...")
    triples, metadata = process_data(texts, nlp)
    
    print(f"Extracted {len(triples)} quality relationships")
    print(get_stats(triples))
    
    # Write to Neo4j
    if triples:
        print("Writing to Neo4j...")
        write_to_neo4j(triples, metadata)
        print("✅ Knowledge graph created successfully!")
    else:
        print("❌ No relationships found to write")

if __name__ == "__main__":
    main()
