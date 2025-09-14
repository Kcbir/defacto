import json
import re
import os

def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""

    # 1. Convert to lowercase
    text = text.lower()

    # 2. Remove URLs
    text = re.sub(r'https?://\S+|www\.\S+', '', text)

    # 3. Remove HTML tags
    text = re.sub(r'<.*?>', '', text)

    # 4. Remove anything that isn't a letter, number, or basic punctuation (.,?!)
    # This helps remove strange characters, control characters, etc.
    text = re.sub(r'[^a-z0-9\s.,?!]', '', text)

    # 5. Remove extra whitespace (multiple spaces, newlines, tabs)
    text = re.sub(r'\s+', ' ', text).strip()

    return text

def chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    words = text.split()
    if not words:
        return []

    chunks = []
    start = 0
    step = chunk_size - overlap

    while start < len(words):
        end = start + chunk_size
        chunk_words = words[start:end]
        chunks.append(" ".join(chunk_words))
        
        # If the last chunk is a full chunk, we are done
        if end >= len(words):
            break
            
        start += step

    return chunks

def process_wikipedia_dump(input_path: str, output_path: str, chunk_size: int, chunk_overlap: int):
    """
    Reads a JSONL file, cleans and chunks the 'text' field, and writes
    the new chunked data to another JSONL file.
    """
    print(f"Starting processing for file: {input_path}")
    
    # Ensure the output directory exists
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    try:
        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:
            
            processed_lines = 0
            total_chunks = 0
            
            for line in infile:
                try:
                    # Load the JSON object from the line
                    data = json.loads(line)
                    
                    # Get the original text, domain, and title
                    original_text = data.get("text", "")
                    domain = data.get("domain")
                    title = data.get("title")

                    # 1. Clean the text
                    cleaned_text = clean_text(original_text)
                    
                    # 2. Chunk the cleaned text
                    chunks = chunk_text(cleaned_text, chunk_size, chunk_overlap)
                    
                    # 3. Write each chunk as a new JSON object to the output file
                    for i, chunk in enumerate(chunks):
                        new_record = {
                            "domain": domain,
                            "title": title,
                            "chunk_id": f"{title}_{i+1}", # Unique ID for the chunk
                            "text": chunk
                        }
                        outfile.write(json.dumps(new_record) + '\n')
                        total_chunks += 1

                    processed_lines += 1
                    if processed_lines % 1000 == 0:
                        print(f"Processed {processed_lines} articles, generated {total_chunks} chunks...")

                except json.JSONDecodeError:
                    print(f"Warning: Skipping a line due to JSON decoding error.")
                    continue

        print("\nProcessing complete!")
        print(f"Total articles processed: {processed_lines}")
        print(f"Total chunks generated: {total_chunks}")
        print(f"Cleaned and chunked data saved to: {output_path}")

    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_path}'")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    # --- Configuration ---
    # Set your input and output file paths here
    INPUT_FILE = '/Users/kabir/Desktop/Research/Implementation/wikipedia_subset.jsonl'
    OUTPUT_FILE = '/Users/kabir/Desktop/Research/Implementation/clean_raw_wiki.jsonl'

    # Set your desired chunking parameters
    CHUNK_SIZE = 256  # Number of words per chunk
    CHUNK_OVERLAP = 32 # Number of words to overlap between chunks

    # --- Run the script ---
    process_wikipedia_dump(INPUT_FILE, OUTPUT_FILE, CHUNK_SIZE, CHUNK_OVERLAP)