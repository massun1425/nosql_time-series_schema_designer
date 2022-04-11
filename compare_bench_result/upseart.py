import sys
import numpy as np
from sklearn.metrics import r2_score
from file_loader import FileLoader
from graph import Graph

# 総実行時間
# 1. 各時刻の各処理の応答時間に実行頻度を掛け合わせる (各処理の加重した応答時間)
# 2. 各グループごとに、グループ内の処理の加重した応答時間の平均値をとる
# 3. 時刻ごとにグループの加重した応答時間の平均値を足し合わせる
# 4. 全ての時刻においてこの値を足し合わせる。


class Upseart:
    @classmethod
    def show_upseart_plan_num(cls, label_grouped_dfs_hash, max_timestep):
        for label in label_grouped_dfs_hash.keys():
            print("=" + label)
            Upseart.__show_upseart_plan_num_each_ts(
                label_grouped_dfs_hash[label], max_timestep)

    @classmethod
    def plot_unweighted_upsert_latency(cls, dir_name, label_dfs_hash, label_grouped_dfs_hash, evaluation_result_column):
        label_data_hash = {}
        for label in label_grouped_dfs_hash.keys():
            insert_statement_nums = len([s for s in label_grouped_dfs_hash[label].keys() if "Aggregate" in s and ("INSERT" in s or "UPDATE" in s)])
            label_data_hash[label] = Upseart.__avg_upseart_latency(label_dfs_hash[label],
                                                                   insert_statement_nums, evaluation_result_column)
        Graph.plot_graph(
            dir_name,
            "Average Insert Latency [s]",
            "timestep",
            "Average Insert Latency[s]",
            label_data_hash, {})


    @classmethod
    def __show_upseart_plan_num_each_ts(cls, statement_dfs_hash, timestep):
        plan_num = [0] * (timestep + 1)
        for statement in statement_dfs_hash:
            if not statement.startswith("Aggregated"):
                continue
            if not (statement.split('_')[1].startswith(
                    "UPDATE") or statement.split('_')[1].startswith("INSERT")):
                continue

            related_dfs = statement_dfs_hash[statement]
            for r_df in related_dfs:
                for t, n in r_df[['timestep', 'name']].values.tolist():
                    plan_num[t] += 1
            print("  --" + statement)
            print("      " + str(plan_num))
            plan_num = [0] * (timestep + 1)

    @classmethod
    def __avg_upseart_latency(cls, df, insert_statement_num, evaluation_result_column):
        values = [[]
                  for _ in range((max(df['timestep'].values.tolist()) + 1))]
        for ts, v in df.query("name.str.startswith(\"UPDATE\")")[
            ['timestep', evaluation_result_column]].values.tolist():
            values[int(ts)].append(v)
        for ts, v in df.query("name.str.startswith(\"INSERT\")")[
            ['timestep', evaluation_result_column]].values.tolist():
            values[int(ts)].append(v)
        if insert_statement_num == 0:
            return [0]
        avg_values = [sum(vs) / insert_statement_num for vs in values]
        return avg_values
