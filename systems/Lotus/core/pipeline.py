import os
import json
import pandas as pd
import numpy as np
from pathlib import Path

import lotus

from lotus.types import CascadeArgs, ProxyModel
from config.settings import settings
from lotus.models import LM
from utils.sql_parser import parse_sql

class LotusPipeline:
    def __init__(self, domain: str, use_cascade: bool = False):
        self.domain = domain
        self.use_cascade = use_cascade
        
        os.environ["GEMINI_API_KEY"] = settings.GEMINI_API_KEY 
        
        model_mini = "gemini/gemini-2.0-flash"
        model_pro = "gemini/gemini-2.5-flash"
        
        self.lm_main = LM(model=model_mini if not use_cascade else model_pro)
        
        if self.use_cascade:
            self.lm_helper = LM(model=model_mini)
            lotus.settings.configure(lm=self.lm_main, helper_lm=self.lm_helper)
        else:
            lotus.settings.configure(lm=self.lm_main)

        self._load_configs()
        self._load_data()

    def _load_configs(self):
        def load_json(filename):
            path = settings.BENCHMARK_DIR / filename
            with open(path, 'r', encoding='utf-8') as f:
                key = self.domain if self.domain != "Art" else "Wiki_Text"
                return json.load(f)[key]
                
        self.extractions = load_json('extractions.json')
        self.descriptions = load_json('descriptions.json')
        self.examples = load_json('examples.json')

    def _load_data(self):
        csv_path = settings.BENCHMARK_DIR / f"ground_truth/{self.domain}.csv"
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Ground truth CSV not found at {csv_path}")
        
        self.df_truth = pd.read_csv(csv_path)
        self.ids = self.df_truth["id"].dropna().astype(str).tolist()[:30]

        base_dir = settings.BENCHMARK_DIR / f"datasets/{self.domain}"
        
        if not os.path.exists(base_dir):
            raise FileNotFoundError(f"Dataset directory not found at {base_dir}")
        
        contexts = []
        for id_value in self.ids:
            file_path = base_dir / f"{id_value}.txt"
            if file_path.exists():
                with open(file_path, "r", encoding="utf-8") as f:
                    contexts.append(f.read().strip())
            else:
                contexts.append('')
                
        self.df_context = pd.DataFrame({'context': contexts})

    def run_sql_task(self, sql: str, output_folder: Path):
        output_folder.mkdir(parents=True, exist_ok=True)
        select_indices, where, attr_indices = parse_sql(sql, self.extractions)
        
        df_target = self.df_context
        filtered_indices = range(len(self.ids))
        
        if where:
            user_inst = "{context}." + "".join([self.descriptions[i] for i in attr_indices]) + where
            
            kwargs = {"strategy": "Cot", "return_all": True, "return_explanations": False}
            if self.use_cascade:
                kwargs["cascade_args"] = CascadeArgs(
                    recall_target=0.9, precision_target=0.9, sampling_percentage=0.5,
                    failure_probability=0.2, proxy_model=ProxyModel.HELPER_LM
                )
            
            filtered_df = df_target.sem_filter(user_inst, **kwargs)
            filtered_indices = filtered_df[filtered_df["filter_label"] == True].index
            df_target = pd.DataFrame({'context': [self.df_context['context'][i] for i in filtered_indices]})

        if self.use_cascade:
            lotus.settings.configure(lm=self.lm_helper)
            
        df_data = {"id": [self.ids[i] for i in filtered_indices]}
        for i in select_indices:
            att = self.extractions[i]
            desc = self.descriptions[i]
            ex_df = pd.DataFrame(self.examples[i])
            
            inst = f"What {att} in {{context}}? {desc} If there are multiple values, separate them with '||' and leave empty if not applicable. Please keep each extracted value concise and avoid lengthy content."
            
            df_mapped = df_target.sem_map(inst, examples=ex_df)
            df_data[att] = df_mapped['_map'].tolist()
            
        # Salvataggio
        df_final = pd.DataFrame(df_data).map(lambda x: np.nan if isinstance(x, str) and "empty" in x else x)
        
        os.makedirs(output_folder, exist_ok=True)
        df_final.to_csv(output_folder / 'results.csv', index=False, encoding='utf-8-sig')