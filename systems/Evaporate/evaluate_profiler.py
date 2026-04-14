from collections import defaultdict, Counter
import numpy as np  # ENHANCED: Required for advanced statistical calculations
import json  # NEW ADDITION: For loading table.json files
import os  # NEW ADDITION: For file path operations
from prompts import (PICK_VALUE_CONTEXT, Step,)
from utils import apply_prompt


def clean_comparison(responses, field):
    clean_responses = []
    # ENHANCED VERSION: Major improvements in handling empty values and edge cases
    # print(f"clean_comparison input: {responses}")  # æ³¨é‡ŠæŽ‰è°ƒè¯•è¾“å‡º
    # å¤„ç†å„ç§ç©ºå€¼æƒ…å†µ
    if responses is None:
        # print("clean_comparison: input is None")
        return ""
    if isinstance(responses, str):
        # print("clean_comparison: input is string")
        if not responses.strip():
            return ""
        responses = [responses]
    elif isinstance(responses, list):
        # print("clean_comparison: input is list")
        # æ‰å¹³åŒ–åµŒå¥—åˆ—è¡¨
        flattened = []
        for item in responses:
            if isinstance(item, list):
                flattened.extend(x for x in item if x and str(x).strip())
            elif item and str(item).strip():
                flattened.append(str(item).strip())
        responses = flattened
        if not responses:
            # print("clean_comparison: flattened list is empty")
            return ""
    # OLD VERSION:
    # if type(responses) == str:
    #     responses = [responses]
    for response in responses:
        if not response or response is None:  # è·³è¿‡ç©ºå€¼
            continue
        response = str(response).lower()
        if response.strip() in ['none', 'null', 'n/a', 'not available', 'not specified', '']:
            continue
        field_lower = field.lower()
        field_reformat = field_lower.replace("_", "-")
        # å¯¹äºŽnameå­—æ®µï¼Œä¸è¦ç§»é™¤å¤ªå¤šå†…å®¹
        if field_lower == "name":
            # åªç§»é™¤æ˜Žæ˜¾çš„å™ªéŸ³å­—ç¬¦ï¼Œä¿ç•™åå­—æœ¬èº«
            for char in ["'", ":", "<", ">", '"', "\n", "\t", "\r"]:
                response = response.replace(char, " ")
        else:
            # å¯¹å…¶ä»–å­—æ®µè¿›è¡Œæ­£å¸¸çš„æ¸…ç†
            for char in ["'", field_lower, field_reformat, ":", "<", ">", '"']:
                response = response.replace(char, " ")
        # æ¸…ç†æ ‡ç‚¹ç¬¦å·å’Œå¤šä½™ç©ºæ ¼
        for char in [",", ".", "?", "!", ";", "(", ")", "[", "]", "{", "}", "-"]:
            response = response.replace(char, " ")
        # è§„èŒƒåŒ–ç©ºæ ¼
        response = " ".join(response.split())
        # åªæœ‰éžç©ºä¸”ä¸æ˜¯æ— æ•ˆå€¼æ—¶æ‰æ·»åŠ 
        if response.strip() and response.lower() not in ['none', 'null', 'n/a', 'not available', 'not specified']:
            clean_responses.append(response.strip())
    result = ", ".join(clean_responses) if clean_responses else ""
    return result
    
    # OLD VERSION:
    # for response in responses:
    #     response = response.lower()
    #     field = field.lower()
    #     field_reformat = field.replace("_", "-")
    #     for char in ["'", field, field_reformat, ":", "<", ">", '"', "none"]:
    #         response = response.replace(char, " ")
    #     for char in [",", ".", "?", "!", ";", "(", ")", "[", "]", "{", "}", "-", "none", "\n", "\t", "\r"]: 
    #         response = response.replace(char, " ")
    #     response = response.replace("  ", " ")
    #     response = response.split()
    #     response = [r.strip() for r in response]
    #     response = [r for r in response if r]
    #     response = ' '.join(response)
    #     clean_responses.append(response)
    # clean_responses = ", ".join(clean_responses)
    # return clean_responses


def normalize_value_type(metadata, attribute):
    # ENHANCED VERSION: Major improvements in handling complex nested data structures and edge cases
    # make everything a list of strings since functions can return diverse types
    cleaned_items = [] 
    
    # å¤„ç†å®Œå…¨ç©ºå€¼çš„æƒ…å†µ
    if metadata is None or metadata == [] or metadata == [[]]:
        return []
    
    # OLD VERSION:
    # if type(metadata) == str:
    #     metadata = [metadata]
    # for item in metadata:
    #     if type(item) == list:
    #         item = [str(i) for i in item]
    #         item = ", ".join(item)
    #     elif type(item) == tuple:
    #         item = list(item)
    #         item = [str(i) for i in item] 
    #         item = ", ".join(item)
    #     elif item is None:
    #         item = ''
    #     elif type(item) != str:
    #         item = [str(item)]
    #         item = ", ".join(item)
    #     if item: 
    #         cleaned_items.append(item)
    # return cleaned_items
    
    if isinstance(metadata, str):
        # æ£€æŸ¥æ˜¯å¦åŒ…å« || åˆ†éš”ç¬¦
        if "||" in metadata:
            metadata = [item.strip() for item in metadata.split("||") if item.strip()]
        else:
            metadata = [metadata]
    elif isinstance(metadata, list):
        # å¤„ç†ç©ºåˆ—è¡¨å’Œ[None]ç­‰ç‰¹æ®Šæƒ…å†µ
        if not metadata or all(x is None or x == '' or x == [] for x in metadata):
            return []
            
        # å¤„ç† [[value]] æ ¼å¼
        if metadata and isinstance(metadata[0], list):
            if not metadata[0] or all(x is None or x == '' for x in metadata[0]):
                return []
            # æŠŠæ‰€æœ‰éžç©ºçš„å€¼éƒ½æ·»åŠ åˆ°ç»“æžœä¸­
            valid_values = []
            for sublist in metadata:
                if isinstance(sublist, list) and sublist:
                    for item in sublist:
                        if item and str(item).strip():
                            valid_values.append(str(item).strip())
            return valid_values if valid_values else []
            
        # å¦‚æžœæ˜¯æ™®é€šåˆ—è¡¨ï¼Œç»§ç»­å¤„ç†
        metadata = [str(x).strip() for x in metadata if x is not None and str(x).strip()]
    
    for item in metadata:
        if isinstance(item, list):
            # å¤„ç†åµŒå¥—åˆ—è¡¨
            if not item:  # ç©ºåˆ—è¡¨
                continue
            item = [str(i) for i in item if i is not None and str(i).strip()]
            item = ", ".join(item)
        elif isinstance(item, tuple):
            item = list(item)
            item = [str(i) for i in item if i is not None and str(i).strip()]
            item = ", ".join(item)
        elif item is None or item == '':
            continue
        elif not isinstance(item, str):
            item = str(item)
        
        # å¯¹å­—ç¬¦ä¸²é¡¹æ£€æŸ¥æ˜¯å¦åŒ…å« || åˆ†éš”ç¬¦
        if isinstance(item, str) and "||" in item:
            sub_items = [sub_item.strip() for sub_item in item.split("||") 
                        if sub_item and sub_item.strip()]
            cleaned_items.extend(sub_items)
            continue
            
        if item and str(item).strip():  # åªæ·»åŠ éžç©ºçš„é¡¹ç›®
            cleaned_items.append(str(item).strip())
    
    # è¿‡æ»¤æŽ‰ç©ºå€¼ã€Noneç­‰
    cleaned_items = [item for item in cleaned_items 
                    if item and item.lower() not in ['none', 'null', 'n/a', '']]
    
    return cleaned_items


def pick_a_gold_label(golds, attribute="", manifest_session=None, overwrite_cache=False):
    """
    To counteract the large model hallucinating on various chunks affecting the evaluation of good functions.
    """

    pred_str = "- " + "\n- ".join(golds)

    prompt_template = PICK_VALUE_CONTEXT[0]
    prompt = prompt_template.format(pred_str=pred_str, attribute=attribute)
    try:
        check, num_toks = apply_prompt(
            Step(prompt), 
            max_toks=100, 
            manifest=manifest_session,
            overwrite_cache=overwrite_cache
        )
    except:
        return golds, 0 
    check = check.split("\n")
    check = [c for c in check if c]
    if check:
        if "none" in check[0].lower():
            check = golds
        else:
            check = check[0]
    return check, num_toks


# ...existing code...

def text_f1(
    preds=[], 
    golds=[], 
    extraction_fraction=1.0, 
    attribute=None,
    extraction_fraction_thresh=0.8,
    use_abstension=True,
    function_name=None,  # æ–°å¢žå‚æ•°
):
    """Compute average F1 of text spans.
    Taken from Squad without prob threshold for no answer.
    è¿‡æ»¤æŽ‰æ ‡å‡†ç­”æ¡ˆä¸ºç©ºçš„æ ·æœ¬ä»¥é¿å…å½±å“å‡½æ•°æ€§èƒ½è¯„ä¼°
    """
    # ENHANCED VERSION: Major improvements in evaluation logic, debugging, and edge case handling
    # Added comprehensive filtering and debugging capabilities
    total_f1 = 0
    total_recall = 0
    total_prec = 0
    f1s = []
    total = 0

    # é¦–å…ˆè¿‡æ»¤æŽ‰æ ‡å‡†ç­”æ¡ˆä¸ºç©ºçš„æ ·æœ¬
    filtered_preds = []
    filtered_golds = []
    
    for pred, gold in zip(preds, golds):
        # æ£€æŸ¥goldæ˜¯å¦ä¸ºç©ºæˆ–æ— æ„ä¹‰
        gold_is_empty = False
        if isinstance(gold, str):
            # æ ‡å‡†åŒ–æ£€æŸ¥ç©ºå€¼
            gold_cleaned = gold.strip().lower()
            if (not gold_cleaned or 
                gold_cleaned in ['', 'none', 'null', 'n/a', 'not available', 'not specified']):
                gold_is_empty = True
        elif isinstance(gold, list):
            # å¯¹äºŽåˆ—è¡¨ç±»åž‹çš„goldï¼Œæ£€æŸ¥æ˜¯å¦æ‰€æœ‰å…ƒç´ éƒ½ä¸ºç©º
            if not gold or all(not str(g).strip() for g in gold):
                gold_is_empty = True
        
        # åªä¿ç•™æ ‡å‡†ç­”æ¡ˆéžç©ºçš„æ ·æœ¬
        if not gold_is_empty:
            filtered_preds.append(pred)
            filtered_golds.append(gold)
    
    # å¦‚æžœè¿‡æ»¤åŽæ²¡æœ‰æœ‰æ•ˆæ ·æœ¬ï¼Œè¿”å›ž0åˆ†
    if not filtered_preds:
        print(f"Warning: All gold labels are empty for attribute '{attribute}', skipping evaluation")
        return 0.0, 0.0
    
    # ä½¿ç”¨è¿‡æ»¤åŽçš„æ•°æ®è¿›è¡ŒåŽŸæœ‰çš„è¯„ä¼°é€»è¾‘
    preds = filtered_preds
    golds = filtered_golds
    
    # ===== æ·»åŠ è°ƒè¯•ä¿¡æ¯å¼€å§‹ =====
    print(f"\n=== text_f1 è°ƒè¯•ä¿¡æ¯ - {attribute}{function_name} ===")
    print(f"è¾“å…¥predsæ•°é‡: {len(preds)}")
    print(f"è¾“å…¥goldsæ•°é‡: {len(golds)}")
    
    for i, (pred, gold) in enumerate(zip(preds[:10], golds[:10])):  # åªæ˜¾ç¤ºå‰10ä¸ª
        print(f"æ ·æœ¬{i}: pred={pred}, gold={gold}")
    # ===== è°ƒè¯•ä¿¡æ¯ç»“æŸ =====

    # OLD VERSION LOGIC (commented out):
    # if extraction_fraction >= extraction_fraction_thresh and use_abstension:
    #     new_preds = []
    #     new_golds = []
    #     for pred, gold in zip(preds, golds):
    #         if pred:
    #             new_preds.append(pred)
    #             new_golds.append(gold)
    #     preds = new_preds
    #     golds = new_golds
    #     if not preds:
    #         return 0.0, 0.0
    # for pred, gold in zip(preds, golds):
    #     if type(pred) == str:
    #         pred_toks = pred.split()
    #     else:
    #         pred_toks = pred
    #     if type(gold) == str:
    #         gold_toks_list = [gold.split()]
    #     else:
    #         assert 0, print(gold)
    #         gold_toks_list = gold

    # if extraction_fraction >= extraction_fraction_thresh and use_abstension:
    #     new_preds = []
    #     new_golds = []
    #     for pred, gold in zip(preds, golds):
    #         if pred:
    #             new_preds.append(pred)
    #             new_golds.append(gold)
    #     preds = new_preds
    #     golds = new_golds
    #     if not preds:
    #         return 0.0, 0.0
    
    # ===== å†æ¬¡æ·»åŠ è°ƒè¯•ä¿¡æ¯ =====
    if extraction_fraction >= extraction_fraction_thresh and use_abstension:
        print(f"è¿‡æ»¤åŽpredsæ•°é‡: {len(preds)}")
        print(f"è¿‡æ»¤åŽgoldsæ•°é‡: {len(golds)}")
        
        for i, (pred, gold) in enumerate(zip(preds[:10], golds[:10])):  # åªæ˜¾ç¤ºå‰10ä¸ª
            print(f"è¿‡æ»¤åŽæ ·æœ¬{i}: pred={pred}, gold={gold}")
    # ===== è°ƒè¯•ä¿¡æ¯ç»“æŸ =====
    
    for pred, gold in zip(preds, golds):
        # æ ‡å‡†åŒ–predï¼Œå¤„ç†å„ç§ç©ºå€¼æƒ…å†µ
        pred_is_empty = False
        
        if pred is None or pred == [] or pred == [[]]:
            pred_is_empty = True
        elif isinstance(pred, list):
            # æ£€æŸ¥[null]æˆ–[[null]]ç­‰æƒ…å†µ
            if all(x is None or x == [] or x == '' for x in pred):
                pred_is_empty = True
            elif len(pred) == 1 and isinstance(pred[0], list):
                # æ£€æŸ¥[[null]]æˆ–[['']]ç­‰æƒ…å†µ
                if all(x is None or x == '' for x in pred[0]):
                    pred_is_empty = True
        elif isinstance(pred, str) and not pred.strip():
            pred_is_empty = True
            
        if pred_is_empty:
            pred_toks = []
        elif isinstance(pred, str):
            pred_toks = pred.split()
        elif isinstance(pred, list) and len(pred) == 1 and isinstance(pred[0], list):
            # å¤„ç†[[value]]æ ¼å¼
            pred_toks = []
            for x in pred[0]:
                if x is not None and str(x).strip():
                    pred_toks.extend(str(x).split())
        else:
            # å¤„ç†å…¶ä»–åˆ—è¡¨æ ¼å¼
            pred_toks = []
            for x in pred:
                if x is not None and str(x).strip():
                    pred_toks.extend(str(x).split())

        # æ ‡å‡†åŒ–gold
        if isinstance(gold, str):
            gold_toks = gold.split()
        else:
            # å¤„ç†åˆ—è¡¨æ ¼å¼çš„gold
            if isinstance(gold, list) and gold:
                if isinstance(gold[0], list):
                    # å¤„ç†[[value]]æ ¼å¼
                    gold_toks = []
                    for sublist in gold:
                        if isinstance(sublist, list):
                            gold_toks.extend([str(x) for x in sublist if x is not None and str(x).strip()])
                        else:
                            if sublist is not None and str(sublist).strip():
                                gold_toks.extend(str(sublist).split())
                else:
                    # å¤„ç†[value]æ ¼å¼
                    gold_toks = []
                    for item in gold:
                        if item is not None and str(item).strip():
                            gold_toks.extend(str(item).split())
            else:
                gold_toks = []
        
        # è®¡ç®—F1åˆ†æ•°
        if not pred_toks:  # é¢„æµ‹ä¸ºç©º
            total_f1 += 0
            f1s.append(0)
            total_recall += 0
            total_prec += 0
            # ===== æ·»åŠ å•ä¸ªæ ·æœ¬è°ƒè¯• =====
            print(f"æ ·æœ¬{total}: predä¸ºç©º, F1=0")
            # ===== è°ƒè¯•ä¿¡æ¯ç»“æŸ =====
        else:
            common = Counter(pred_toks) & Counter(gold_toks)
            num_same = sum(common.values())
            if num_same == 0:  # æ²¡æœ‰å…±åŒå…ƒç´ 
                total_f1 += 0
                f1s.append(0)
                total_recall += 0
                total_prec += 0
                # ===== æ·»åŠ å•ä¸ªæ ·æœ¬è°ƒè¯• =====
                print(f"æ ·æœ¬{total}: æ— å…±åŒå…ƒç´ , F1=0, pred_toks={pred_toks}, gold_toks={gold_toks}")
                # ===== è°ƒè¯•ä¿¡æ¯ç»“æŸ =====                    
            else:
                precision = 1.0 * num_same / len(pred_toks)
                recall = 1.0 * num_same / len(gold_toks)
                f1 = (2 * precision * recall) / (precision + recall)
                total_f1 += f1
                total_recall += recall
                total_prec += precision
                f1s.append(f1)
                # ===== æ·»åŠ å•ä¸ªæ ·æœ¬è°ƒè¯• =====
                print(f"æ ·æœ¬{total}: F1={f1:.4f}, P={precision:.4f}, R={recall:.4f}, pred_toks={pred_toks}, gold_toks={gold_toks}")
                # ===== è°ƒè¯•ä¿¡æ¯ç»“æŸ =====

        total += 1

    # æ€»æ ·æœ¬æ•°ä¸º0æ—¶è¿”å›ž0åˆ†
    if not total:
        print("Warning: No valid samples for evaluation")
        return 0.0, 0.0
    
    # è®¡ç®—å¹³å‡åˆ†æ•°    
    f1_avg = total_f1 / total
    f1_median = np.percentile(f1s, 50) if f1s else 0.0
    
    # ===== æ·»åŠ æœ€ç»ˆç»“æžœè°ƒè¯• =====
    print(f"=== æœ€ç»ˆè®¡ç®—ç»“æžœ ===")
    print(f"æ€»æ ·æœ¬æ•°: {total}")
    print(f"å¹³å‡F1: {f1_avg:.4f}")
    print(f"ä¸­ä½æ•°F1: {f1_median:.4f}")
    print(f"F1åˆ†æ•°åˆ†å¸ƒ: {Counter([round(x, 2) for x in f1s])}")
    print(f"=== è°ƒè¯•ç»“æŸ ===\n")
    # ===== è°ƒè¯•ä¿¡æ¯ç»“æŸ =====
    
    return f1_avg, f1_median

def evaluate(
    all_extractions:list,
    gold_key:str, 
    field:str, 
    manifest_session=None, 
    overwrite_cache=False, 
    combiner_mode='mv',
    extraction_fraction_thresh=0.8,
    use_abstension=True,
    gold_extractions_file=None,  # NEW PARAMETER: Support for external gold standard files
):
    # ENHANCED VERSION: Added dynamic table.json loading and comprehensive gold standard handling
    # åŠ¨æ€åŠ è½½table.jsonä½œä¸ºæ ‡å‡†ç­”æ¡ˆ
    if gold_key == "gpt-4.1-mini":
        # åªä½¿ç”¨ä¼ å…¥çš„gold_extractions_fileè·¯å¾„ï¼Œä¸å›žé€€åˆ°lcr
        possible_table_paths = []
        if gold_extractions_file:
            possible_table_paths.append(gold_extractions_file)
            print(f"Using specified gold_extractions_file: {gold_extractions_file}")
        else:
            print("WARNING: No gold_extractions_file specified, will use empty gold standard")
        
        standard_answers_dict = {}
        table_json_path = None
        
        for path in possible_table_paths:
            if os.path.exists(path):
                table_json_path = path
                break
        
        if table_json_path:
            try:
                with open(table_json_path, 'r', encoding='utf-8') as f:
                    standard_answers_dict = json.load(f)
                print(f"æˆåŠŸåŠ è½½table.json: {table_json_path}")
                print(f"æ€»å…±åŠ è½½äº† {len(standard_answers_dict)} ä¸ªæ–‡ä»¶çš„æ ‡å‡†ç­”æ¡ˆ")
            except Exception as e:
                print(f"åŠ è½½table.jsonå¤±è´¥: {e}")
                standard_answers_dict = {}
        else:
            print("æœªæ‰¾åˆ°table.jsonæ–‡ä»¶ï¼Œä½¿ç”¨ç©ºçš„æ ‡å‡†ç­”æ¡ˆå­—å…¸")
        
        # æ·»åŠ è°ƒè¯•ä¿¡æ¯
        print(f"å½“å‰å­—æ®µ: {field}")
        print(f"all_extractionsä¸­çš„æ–‡ä»¶è·¯å¾„ç¤ºä¾‹:")
        if gold_key in all_extractions:
            sample_paths = list(all_extractions[gold_key].keys())[:3]
            for path in sample_paths:
                print(f"  {path}")
        
        print(f"æ ‡å‡†ç­”æ¡ˆå­—å…¸ä¸­çš„æ–‡ä»¶è·¯å¾„ç¤ºä¾‹:")
        sample_standard_paths = list(standard_answers_dict.keys())[:3]
        for path in sample_standard_paths:
            print(f"  {path}")
        
        # æ ¹æ®å½“å‰fieldåŠ¨æ€æå–å¯¹åº”çš„æ ‡å‡†ç­”æ¡ˆ
        your_gold_answers = {}
        
        # èŽ·å–åŽŸå§‹çš„gpt-4.1-miniæ•°æ®ä»¥äº†è§£è·¯å¾„æ ¼å¼
        original_gold_answers = all_extractions.get(gold_key, {})
        
        for file_path in original_gold_answers.keys():
            # å°è¯•å¤šç§è·¯å¾„åŒ¹é…æ–¹å¼
            matched_value = None
            
            # 1. ç›´æŽ¥åŒ¹é…
            if file_path in standard_answers_dict:
                matched_value = standard_answers_dict[file_path].get(field, "")
                print(f"ç›´æŽ¥åŒ¹é…: {file_path} -> {matched_value}")
            
            # 2. æ–‡ä»¶ååŒ¹é…
            else:
                file_name = file_path.split('/')[-1]  # èŽ·å–æ–‡ä»¶å
                for std_path, attributes in standard_answers_dict.items():
                    if std_path.endswith(file_name):
                        matched_value = attributes.get(field, "")
                        print(f"æ–‡ä»¶ååŒ¹é…: {file_path} -> {std_path} -> {matched_value}")
                        break
            
            # 3. å¦‚æžœè¿˜æ˜¯æ²¡æ‰¾åˆ°ï¼Œä½¿ç”¨åŽŸå§‹å€¼
            if matched_value is None:
                matched_value = ""
                print(f"æœªæ‰¾åˆ°åŒ¹é…: {file_path} -> ä½¿ç”¨ç©ºå€¼")
            
            # æ ¼å¼åŒ–ä¸º[[value]]çš„æ ¼å¼
            your_gold_answers[file_path] = [[matched_value]]
        
        # æ›¿æ¢åŽŸæœ‰çš„gpt-4.1-miniæ•°æ®
        all_extractions[gold_key] = your_gold_answers
        print(f"æ›¿æ¢äº† {len(your_gold_answers)} ä¸ªæ–‡ä»¶çš„é‡‘æ ‡å‡†")
        
    normalized_field_name = field
    for char in ["'", ":", "<", ">", '"', "_", "-", " ", "none"]:
        normalized_field_name = normalized_field_name.replace(char, "")

    key2golds = defaultdict(list)
    key2preds = defaultdict(list)
    total_tokens_prompted = 0

    # handle FM golds on D_eval
    gold_file2metadata = all_extractions[gold_key]
    cleaned_gold_metadata = {}
    for filepath, gold_metadata in gold_file2metadata.items():
        #print(f"\nå¤„ç†é‡‘æ ‡å‡† {filepath}:")
        #print(f"åŽŸå§‹é‡‘æ ‡å‡†: {gold_metadata}")
        
        normalized_gold = normalize_value_type(gold_metadata, field)
        #print(f"normalize_value_type åŽ: {normalized_gold}")
        
        if len(normalized_gold) > 1:
            normalized_gold, num_toks = pick_a_gold_label(
                normalized_gold, 
                attribute=field, 
                manifest_session=manifest_session, 
                overwrite_cache=overwrite_cache
            )
            total_tokens_prompted += num_toks
            #print(f"pick_a_gold_label åŽ: {normalized_gold}")
        
        cleaned_gold = clean_comparison(normalized_gold, field)
        #print(f"clean_comparison åŽ: {cleaned_gold}")
        
        cleaned_gold_metadata[filepath] = cleaned_gold
    
    # handle function preds on D_eval
    # åœ¨è¿™é‡Œä¹Ÿè¿‡æ»¤æŽ‰æ ‡å‡†ç­”æ¡ˆä¸ºç©ºçš„æ–‡ä»¶
    for i, (key, file2metadata) in enumerate(all_extractions.items()):
        if key == gold_key:
            continue
        for filepath, metadata in file2metadata.items():
            gold_metadata = cleaned_gold_metadata[filepath]
            
            # æ£€æŸ¥é‡‘æ ‡å‡†æ˜¯å¦ä¸ºç©ºï¼Œå¦‚æžœä¸ºç©ºåˆ™è·³è¿‡è¯¥æ–‡ä»¶
            gold_is_empty = False
            if isinstance(gold_metadata, str):
                gold_cleaned = gold_metadata.strip().lower()
                if (not gold_cleaned or 
                    gold_cleaned in ['', 'none', 'null', 'n/a', 'not available', 'not specified']):
                    gold_is_empty = True
            elif isinstance(gold_metadata, list):
                if not gold_metadata or all(not str(g).strip() for g in gold_metadata):
                    gold_is_empty = True
            elif gold_metadata is None:
                gold_is_empty = True
            
            # è®°å½•å¹¶è¾“å‡ºæ¯ä¸ªæ–‡ä»¶çš„å¤„ç†çŠ¶æ€
            # print(f"\nProcessing file: {filepath}")
            # print(f"Original gold metadata: {gold_metadata}")
            # print(f"Is gold empty: {gold_is_empty}")
            
            # åªæœ‰å½“é‡‘æ ‡å‡†éžç©ºæ—¶æ‰åŠ å…¥è¯„ä¼°
            if not gold_is_empty:
                pred_metadata = normalize_value_type(metadata, field)
                cleaned_pred = clean_comparison(pred_metadata, field)
                # print(f"Original prediction: {metadata}")
                # print(f"Normalized prediction: {pred_metadata}")
                # print(f"Cleaned prediction: {cleaned_pred}")
                
                # æ·»åŠ åˆ°è¯„ä¼°é›†åˆ
                key2golds[key].append(gold_metadata)
                key2preds[key].append(cleaned_pred)
            else:
                print("Skipping due to empty gold standard")

    # Handling abstensions
    metrics = {}
    for key, gold_list in key2golds.items():
        if not gold_list:  # å¦‚æžœæ²¡æœ‰æœ‰æ•ˆçš„æ ·æœ¬
            print(f"Warning: No valid samples for key {key} and field {field}")
            metrics[key] = {
                "average_f1": 0.0,
                "median_f1": 0.0,
                "extraction_fraction": 0.0,
                "prior_average_f1": 0.0,
                "prior_median_f1": 0.0,
            }
            continue
            
        num_extractions = 0
        for gold_item in gold_list:
            if gold_item and not any(gold_item.lower() == wd for wd in ['none']):
                num_extractions += 1
        extraction_fraction = float(num_extractions) / float(len(gold_list))
        if combiner_mode == "top_k":
            # Don't use the extraction fraction in the naive setting for scoring
            extraction_fraction = 0.0
        
        preds = key2preds[key]
        f1, f1_med = text_f1(
            preds, gold_list, 
            extraction_fraction=extraction_fraction, 
            attribute=field,
            extraction_fraction_thresh=extraction_fraction_thresh,
            use_abstension=use_abstension,
            function_name=key,  # ä¼ é€’å‡½æ•°å
        )
        priorf1, priorf1_med = text_f1(preds, gold_list, extraction_fraction=0.0, attribute=field,function_name=key)  # ä¼ é€’å‡½æ•°å
        metrics[key] = {
            "average_f1": f1,
            "median_f1": f1_med,
            "extraction_fraction": extraction_fraction,
            "prior_average_f1": priorf1,
            "prior_median_f1": priorf1_med,
            
        } 

    return metrics, key2golds, total_tokens_prompted

# ...existing code...


# ...existing code...

def get_topk_scripts_per_field(
    script2metrics,
    function_dictionary,
    all_extractions,
    gold_key='',
    k=3,
    do_end_to_end=False,
    keep_thresh=0.1,
    cost_thresh=1,
    combiner_mode='mv',
):
    def _simple_clean(v):
        if v is None:
            return ""
        s = str(v).strip().lower()
        if s in {"", "none", "null", "n/a", "not available"}:
            return ""
        return " ".join(s.split())

    all_function_keys = [key for key in script2metrics.keys() if key != gold_key and "function" in key]

    if not all_function_keys:
        all_keys = [key for key in script2metrics.keys() if key != gold_key]
        if all_keys:
            best_key = max(all_keys, key=lambda x: script2metrics[x].get("average_f1", 0.0))
            print(f"No functions available, using best model: {best_key}")
            return [best_key]
        return []

    ranked = []
    for key in all_function_keys:
        metrics = script2metrics.get(key, {})
        avg_f1 = float(metrics.get("average_f1", 0.0))
        med_f1 = float(metrics.get("median_f1", 0.0))
        extraction_fraction = float(metrics.get("extraction_fraction", 0.0))

        preds_map = all_extractions.get(key, {}) if isinstance(all_extractions, dict) else {}
        cleaned_preds = [_simple_clean(v) for v in preds_map.values()] if isinstance(preds_map, dict) else []
        non_empty = [v for v in cleaned_preds if v]
        non_empty_ratio = (len(non_empty) / len(cleaned_preds)) if cleaned_preds else 0.0
        unique_non_empty = len(set(non_empty)) if non_empty else 0
        avg_len = (sum(len(v) for v in non_empty) / len(non_empty)) if non_empty else 0.0

        score = (0.60 * avg_f1) + (0.20 * med_f1) + (0.20 * extraction_fraction)

        if non_empty_ratio < 0.08:
            score -= 0.35
        elif non_empty_ratio < 0.20:
            score -= 0.15

        if unique_non_empty <= 1 and non_empty_ratio > 0.50:
            score -= 0.25

        if avg_len > 160:
            score -= 0.10

        ranked.append(
            {
                "key": key,
                "score": score,
                "avg_f1": avg_f1,
                "median_f1": med_f1,
                "extraction_fraction": extraction_fraction,
                "non_empty_ratio": non_empty_ratio,
                "unique_non_empty": unique_non_empty,
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    sorted_functions = [r["key"] for r in ranked]

    if k == -1:
        dynamic_threshold = max(0.02, ranked[0]["score"] - 0.08) if ranked else 0.02
        selected_functions = [r["key"] for r in ranked if r["score"] >= dynamic_threshold]
        if not selected_functions:
            selected_functions = sorted_functions[:3]
    else:
        selected_functions = sorted_functions[:k]

    if ranked:
        best = ranked[0]
        print(
            "Best script overall: "
            f"{best['key']}; score={best['score']:.4f}; "
            f"avg_f1={best['avg_f1']:.4f}; ext_frac={best['extraction_fraction']:.4f}"
        )
        print(f"Selected functions: {selected_functions}")

    return selected_functions


