RELATIONS = {
    'founded': ('FOUNDED', 9), 'established': ('FOUNDED', 9), 'created': ('FOUNDED', 8),
    'owns': ('OWNS', 8), 'controls': ('OWNS', 7), 'acquired': ('ACQUIRED', 9),
    'works': ('WORKS_FOR', 6), 'leads': ('LEADS', 7), 'manages': ('LEADS', 6),
    'located': ('LOCATED_IN', 6), 'based': ('LOCATED_IN', 6), 'headquartered': ('LOCATED_IN', 7),
    'teaches': ('TEACHES', 6), 'studies': ('STUDIES', 5), 'develops': ('DEVELOPS', 6)
}

def find_entity_for_token(token, entities):
    """Find which entity contains this token"""
    for ent_text, ent_type in entities:
        # Check if token is part of this entity
        if token.text.lower() in ent_text.lower():
            return (ent_text, ent_type)
    return None

def find_relationships(text, entities, nlp):
    """Find quality relationships using spaCy dependency parsing"""
    if len(entities) < 2:
        return []
        
    doc = nlp(text[:3000])  # Slightly larger for better context
    triples = []
    
    # Rule 1: Subject-Verb-Object patterns (most reliable)
    for token in doc:
        if token.pos_ == "VERB" and not token.is_stop:
            verb = token.lemma_.lower()
            relation_info = RELATIONS.get(verb, ('RELATED_TO', 3))
            relation, score = relation_info
            
            if score < 5:  # Skip low-quality relations
                continue
                
            subjects = []
            objects = []
            
            # Find subjects and objects using dependencies
            for child in token.children:
                if child.dep_ in ["nsubj", "nsubjpass"]:  # Subject
                    subj_ent = find_entity_for_token(child, entities)
                    if subj_ent:
                        subjects.append(subj_ent)
                        
                elif child.dep_ in ["dobj", "pobj"]:  # Direct/prepositional object
                    obj_ent = find_entity_for_token(child, entities)
                    if obj_ent:
                        objects.append(obj_ent)
                        
                elif child.dep_ == "prep":  # Prepositional phrases
                    for grandchild in child.children:
                        if grandchild.dep_ == "pobj":
                            obj_ent = find_entity_for_token(grandchild, entities)
                            if obj_ent:
                                objects.append(obj_ent)
            
            # Create triples
            for subj in subjects:
                for obj in objects:
                    if subj != obj and subj[0] != obj[0]:
                        triples.append((subj[0], relation, obj[0], score))
    
    # Rule 2: "is/are" relationships for definitions
    for token in doc:
        if token.lemma_ == "be" and token.pos_ in ["AUX", "VERB"]:
            subjects = []
            predicates = []
            
            for child in token.children:
                if child.dep_ == "nsubj":
                    subj_ent = find_entity_for_token(child, entities)
                    if subj_ent:
                        subjects.append(subj_ent)
                elif child.dep_ in ["attr", "acomp"]:
                    pred_ent = find_entity_for_token(child, entities)
                    if pred_ent:
                        predicates.append(pred_ent)
            
            for subj in subjects:
                for pred in predicates:
                    if subj != pred and subj[0] != pred[0]:
                        triples.append((subj[0], "IS_A", pred[0], 6))
    
    # Rule 3: Possessive relationships
    for token in doc:
        if token.dep_ == "poss":  # Possessive
            possessor = find_entity_for_token(token, entities)
            possessed = find_entity_for_token(token.head, entities)
            if possessor and possessed and possessor != possessed:
                triples.append((possessor[0], "OWNS", possessed[0], 5))
    
    # Filter and deduplicate
    seen = set()
    quality_triples = []
    for triple in triples:
        if len(triple) >= 4:
            subj, rel, obj, score = triple
            key = (subj.lower(), rel, obj.lower())
            if key not in seen and score >= 5:  # Quality threshold
                seen.add(key)
                quality_triples.append((subj, rel, obj))
    
    return quality_triples
