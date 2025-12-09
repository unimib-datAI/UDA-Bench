#!/usr/bin/env python3
"""
Script to refine *_evidence columns in CSV files by extracting the most relevant
substring from the original evidence text without any summarization or paraphrasing.
"""
import pandas as pd
import openai
import os
import re
from typing import Tuple, Optional
import sys


OPENAI_API_KEY = "sk-fthhzHHMmUwA5cDq0eC213365c824c4f80B588C3E1557eB2"
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY



def find_evidence_heuristic(value: str, evidence: str, attribute_name: str) -> str:
    """
    Fallback heuristic method to find relevant evidence substring.
    """
    if not evidence or not evidence.strip():
        return evidence
    
    value_str = str(value).strip()
    if not value_str or value_str == "nan":
        return evidence
    
    # Split evidence into sentences, preserving punctuation
    sentence_pattern = r'[.!?]+\s*'
    sentences = re.split(sentence_pattern, evidence)
    
    # Find sentence boundaries in the original text
    sentence_spans = []
    start = 0
    for sentence in sentences:
        if sentence.strip():
            # Find this sentence in the original evidence
            sentence_start = evidence.find(sentence.strip(), start)
            if sentence_start != -1:
                sentence_end = sentence_start + len(sentence.strip())
                # Include any punctuation after the sentence
                while sentence_end < len(evidence) and evidence[sentence_end] in '.!?':
                    sentence_end += 1
                sentence_spans.append((sentence_start, sentence_end))
                start = sentence_end
    
    # Look for sentences that mention the value (case-insensitive)
    best_span = None
    best_sentence = ""
    
    for start_idx, end_idx in sentence_spans:
        sentence_text = evidence[start_idx:end_idx].strip()
        if sentence_text and value_str.lower() in sentence_text.lower():
            if not best_sentence or len(sentence_text) < len(best_sentence):
                best_sentence = sentence_text
                best_span = (start_idx, end_idx)
    
    # If we found a good sentence, return it
    if best_span:
        return evidence[best_span[0]:best_span[1]]
    
    # If no sentence mentions the value, look for any occurrence of the value
    value_pos = evidence.lower().find(value_str.lower())
    if value_pos != -1:
        # Find the sentence containing this position
        for start_idx, end_idx in sentence_spans:
            if start_idx <= value_pos < end_idx:
                return evidence[start_idx:end_idx]
    
    # If still no match, return the first sentence or the full evidence if it's short
    if len(evidence) <= 200:
        return evidence
    else:
        if sentence_spans:
            return evidence[sentence_spans[0][0]:sentence_spans[0][1]]
        else:
            # No sentence structure found, return first 200 chars
            return evidence[:200] + "..."

def find_best_evidence_substring_with_llm(value: str, evidence: str, attribute_name: str) -> str:
    """
    Use an LLM to find the best substring of evidence that supports the given value.
    Returns the substring of evidence that best supports the value.
    """
    if not evidence or not evidence.strip():
        return evidence
    
    value_str = str(value).strip()
    if not value_str or value_str == "nan":
        return evidence
    
    # Create prompt for LLM to identify the best supporting span
    prompt = f"""Given the following evidence text and an attribute value, identify the shortest contiguous substring from the evidence that best supports or describes the attribute value.

Attribute: {attribute_name}
Value: {value_str}

Evidence text:
{evidence}

Instructions:
1. Find the shortest span (preferably a complete sentence) in the evidence that directly supports, mentions, or describes the value "{value_str}"
2. The span must be a contiguous substring of the original evidence text - no modifications allowed
3. Respond with ONLY the exact substring from the evidence text
4. If the value is not mentioned in the evidence, return the shortest relevant sentence
5. Prefer complete sentences when possible

Response (exact substring only):"""

    try:
        # Check if OpenAI API key is available
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("Warning: OPENAI_API_KEY not found. Using heuristic method.")
            return find_evidence_heuristic(value_str, evidence, attribute_name)
        
        client = openai.OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="qwen3-vl-235b-a22b-instruct",
            messages=[
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.1
        )
        
        suggested_substring = response.choices[0].message.content.strip()
        
        # Remove any quotes that the LLM might have added
        if suggested_substring.startswith('"') and suggested_substring.endswith('"'):
            suggested_substring = suggested_substring[1:-1]
        
        # Verify that the suggested substring is actually in the original evidence
        if suggested_substring and suggested_substring in evidence:
            return suggested_substring
        else:
            # If LLM response isn't a valid substring, fall back to heuristic
            print(f"Warning: LLM suggested substring not found in evidence for {attribute_name}={value_str}. Using heuristic.")
            return find_evidence_heuristic(value_str, evidence, attribute_name)
            
    except Exception as e:
        print(f"Error calling LLM for {attribute_name}={value_str}: {e}")
        # Fall back to heuristic
        return find_evidence_heuristic(value_str, evidence, attribute_name)

def refine_evidence_csv(input_file: str, output_file: str, use_llm: bool = True):
    """
    Main function to refine evidence columns in the CSV.
    """
    # Read the CSV file
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    
    # Find attribute columns (not containing _evidence, _tag, or _reason)
    attribute_columns = []
    for col in df.columns:
        if not any(suffix in col for suffix in ['_evidence', '_tag', '_reason']):
            # Check if there's a corresponding evidence column
            evidence_col = f"{col}_evidence"
            if evidence_col in df.columns:
                attribute_columns.append(col)
    
    print(f"Found {len(attribute_columns)} attribute columns with evidence: {attribute_columns}")
    
    if not attribute_columns:
        print("No attribute columns found. Exiting.")
        return
    
    # Process each attribute column
    total_refined = 0
    for attr in attribute_columns:
        evidence_col = f"{attr}_evidence"
        refined_col = f"{attr}_evidence_refined"
        
        # Create new column for refined evidence, positioned right after the original evidence column
        evidence_col_idx = df.columns.get_loc(evidence_col)
        
        # Initialize the refined column with empty strings
        df.insert(evidence_col_idx + 1, refined_col, "")
        
        print(f"\nProcessing attribute: {attr}")
        print(f"Created new column: {refined_col}")
        
        for idx, row in df.iterrows():
            value = row[attr] if pd.notna(row[attr]) else ""
            evidence = str(row[evidence_col]) if pd.notna(row[evidence_col]) else ""
            
            if value != "" and evidence and str(value) != "nan":
                print(f"  Row {idx}: Refining evidence for {attr} = '{str(value)[:50]}{'...' if len(str(value)) > 50 else ''}'")
                
                if use_llm:
                    refined_evidence = find_best_evidence_substring_with_llm(value, evidence, attr)
                else:
                    refined_evidence = find_evidence_heuristic(str(value), evidence, attr)
                
                # Store refined evidence in the new column
                df.at[idx, refined_col] = refined_evidence
                
                # Print some info about the refinement
                original_len = len(evidence)
                refined_len = len(refined_evidence)
                print(f"    Reduced from {original_len} to {refined_len} characters ({refined_len/original_len*100:.1f}% of original)")
                total_refined += 1
            else:
                # For rows without valid value or evidence, keep the refined column empty
                df.at[idx, refined_col] = ""
    
    # Save the refined CSV
    try:
        df.to_csv(output_file, index=False)
        print(f"\nSuccessfully saved refined CSV to: {output_file}")
        print(f"Total refined evidence entries: {total_refined}")
    except Exception as e:
        print(f"Error saving CSV file: {e}")

def main():
    """
    Main entry point with command line argument handling.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Refine evidence columns in CSV by extracting relevant substrings")
    parser.add_argument("input_file", help="Input CSV file path")
    parser.add_argument("-o", "--output", help="Output CSV file path (default: add '_refined' to input filename)")
    parser.add_argument("--no-llm", action="store_true", help="Use heuristic method only, don't call LLM API")
    
    args = parser.parse_args()
    
    input_file = args.input_file
    
    # Generate output filename if not provided
    if args.output:
        output_file = args.output
    else:
        base_name = os.path.splitext(input_file)[0]
        ext = os.path.splitext(input_file)[1]
        output_file = f"{base_name}_refined{ext}"
    
    use_llm = not args.no_llm
    
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Using LLM: {use_llm}")
    
    if use_llm and not os.getenv("OPENAI_API_KEY"):
        print("\nWarning: OPENAI_API_KEY environment variable not set.")
        print("Either set the API key or use --no-llm flag to use heuristic method only.")
        response = input("Continue with heuristic method? (y/n): ")
        if response.lower() != 'y':
            print("Exiting.")
            return
        use_llm = False
    
    refine_evidence_csv(input_file, output_file, use_llm)

if __name__ == "__main__":
    # For direct execution with hardcoded files
    if len(sys.argv) == 1:
        input_file = "/data/dengqiyan/UDA-Bench/Evidence/Player/manager.csv"
        output_file = "/data/dengqiyan/UDA-Bench/Evidence/Player/manager_refined.csv"
        
        print("Running with default files:")
        print(f"Input: {input_file}")
        print(f"Output: {output_file}")
        print()
        
        # Check if we should use LLM or heuristic
        use_llm = bool(os.getenv("OPENAI_API_KEY"))
        if not use_llm:
            print("OPENAI_API_KEY not found. Using heuristic method only.")
        
        refine_evidence_csv(input_file, output_file, use_llm)
    else:
        main()
