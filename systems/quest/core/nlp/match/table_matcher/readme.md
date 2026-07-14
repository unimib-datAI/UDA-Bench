# QUEST table matching utilities

Run examples with `systems/quest` on `PYTHONPATH` (the normal QUEST entrypoint already provides this import context). Complete runnable examples are in `demo/`.

## Match two aligned tables

`modular_matcher.py` exposes configurable exact, edit-distance, semantic-similarity and optional LLM-judge strategies.

```python
from core.nlp.match.table_matcher.modular_matcher import create_semantic_matcher

matcher = create_semantic_matcher()
results = matcher.match_tables(
    ground_truth=ground_truth_df,
    llm_extract=extracted_df,
    primary_keys="id",                 # or a list for a composite key
    column_types={"name": "STRING"},
)
matcher.print_results(results)
```

A custom matcher can be built with `MatchingConfig`:

```python
from core.nlp.match.table_matcher.modular_matcher import MatchingConfig, ModularTableMatcher

config = (
    MatchingConfig()
    .add_exact_match(priority=0)
    .add_edit_distance(threshold=0.6, priority=1)
    .add_semantic_similarity(threshold=0.7, priority=2)
    .set_fusion_mode("priority")        # priority, voting, or weighted
)
matcher = ModularTableMatcher(config)
```

Semantic matching loads `intfloat/multilingual-e5-large` by default. Use only exact/edit-distance strategies when that model is unavailable.

## Fuzzy joins

`table_join.py` provides `pd_join_by_column` and `pd_join_by_column_with_join_type`; the latter accepts `inner`, `left`, `right`, or `outer`.

```python
from core.nlp.match.table_matcher.table_join import (
    create_advanced_join_matcher,
    pd_join_by_column_with_join_type,
)

joined = pd_join_by_column_with_join_type(
    left_table=left_df,
    right_table=right_df,
    left_column_name="company",
    right_column_name="company_name",
    column_type="STRING",
    join_type="inner",
    matcher=create_advanced_join_matcher(),
)
```

See `demo/modular_demo.py`, `demo/join_demo.py`, and `demo/multivalue_join_demo.py` for broader examples.
