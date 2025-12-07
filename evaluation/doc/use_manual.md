先用evaluation/sql_preprocessor.py 将对应的sql语句以及相关的属性描述收集到json文件中。
再指定这个json文件的路径，交给 evaluation/run_eval.py 脚本去算acc。

比如： player数据集的join:

python3 -m evaluation.sql_preprocessor \
  --dataset Player \
  --task Join \
  --sql-file Query/Player/Join/join_queries.sql \
  --attributes-file Query/Player/Player_attributes.json \
  --output-root evaluation/demo_acc_result 

# Player Join
python3 -m evaluation.run_eval \
  --dataset Player \
  --task Join \
  --sql-file evaluation/demo_acc_result/Player/Join/join_queries/2/sql.json \
  --result-csv evaluation/demo_acc_result/Player/Join/join_queries/2/join_player_2.csv
