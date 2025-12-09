#!/usr/bin/env python3
"""
调试表名设置
"""

import os
import sys
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from build_join import create_player_join_graph

def debug_tables():
    """调试表名设置"""
    base_path = "/data/dengqiyan/UDA-Bench/Query/Player"
    
    print("创建join graph...")
    join_graph = create_player_join_graph(base_path)
    
    print("\n表配置检查:")
    for table_name, table_config in join_graph.tables.items():
        print(f"\n表 '{table_name}':")
        print(f"  CSV路径: {table_config.csv_path}")
        print(f"  属性数量: {len(table_config.attributes)}")
        
        if len(table_config.attributes) > 0:
            print(f"  前3个属性:")
            for i, attr in enumerate(table_config.attributes[:3]):
                print(f"    {i+1}. {attr.name} (table: '{attr.table}')")
    
    print(f"\n数据统计键: {list(join_graph.data_stats.keys())}")

if __name__ == "__main__":
    debug_tables()
