"""
Enhanced Entity Extractor - Quality entity recognition with filtering
"""
import spacy
import re

def load_nlp():
    """Load best available spacy model"""
    try:
        nlp = spacy.load("en_core_web_lg")
        print("Loaded large spaCy model")
        return nlp
    except OSError:
        try:
            nlp = spacy.load("en_core_web_sm")
            print("Loaded small spaCy model")
            return nlp
        except OSError:
            raise RuntimeError("Install: python -m spacy download en_core_web_sm")

def clean_entity_text(text):
    """Clean and normalize entity text"""
    text = re.sub(r'\s+', ' ', text.strip())
    text = re.sub(r'[^\w\s\-\.]', '', text)
    return text

def is_quality_entity(text, ent_type):
    """Filter for quality entities"""
    clean_text = clean_entity_text(text)
    
    # Basic quality checks
    if (len(clean_text) < 3 or  # Too short
        clean_text.isdigit() or  # Just numbers
        clean_text.lower() in ['the', 'a', 'an', 'this', 'that'] or  # Stop words
        ent_type in ['CARDINAL', 'ORDINAL']):  # Numbers
        return False
    
    # Prefer important entity types
    important_types = ['PERSON', 'ORG', 'GPE', 'LOC', 'PRODUCT', 'EVENT', 'FAC']
    return ent_type in important_types

def extract_entities(text, nlp):
    """Extract quality entities with proper filtering"""
    doc = nlp(text[:3000])  # Reasonable limit
    entities = []
    seen = set()
    
    for ent in doc.ents:
        clean_text = clean_entity_text(ent.text)
        
        if (is_quality_entity(clean_text, ent.label_) and 
            clean_text.lower() not in seen):
            entities.append((clean_text, ent.label_))
            seen.add(clean_text.lower())
    
    return entities
