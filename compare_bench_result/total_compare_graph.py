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


def plot_queries(
        dir_name,
        label_dfs_hash):
    for statement in list(label_dfs_hash.values())[0].keys():
        if statement.split('_')[1].startswith(
                "UPDATE") or statement.split('_')[1].startswith("INSERT"):
            continue
            #label_data_hash = {}
            #label_se_hash = {}
            #for label in label_dfs_hash.keys():
            #    if statement in label_dfs_hash[label]:
            #        related_dfs1 = label_dfs_hash[label][statement]
            #        label_data_hash[label] = sumup_each_series(
            #            max_ts, related_dfs1)
            #        if 'standard_error' in label_dfs_hash[label][statement][0].columns:
            #            label_se_hash[label] = list(
            #                label_dfs_hash[label][statement][0]['standard_error'].values.tolist())
            #Graph.plot_graph(statement, 'timesteps', 'Latency [s]', label_data_hash, label_se_hash)
        else:
            if statement.split('_')[1].startswith("SELECT"):
                plot_statement(dir_name, label_dfs_hash, statement, evaluation_result_column, statement.split("--")[1], 'Latency [s]', True)
                plot_statement(dir_name, label_dfs_hash, statement, 'cost', "COST" + "\n" + statement, 'Estimated Cost', False)
                continue
            elif "TOTAL_TOTAL" == statement:
            #    plot_statement(label_dfs_hash, statement, evaluation_result_column,"Frequency-weighted Total Latency [s]", 'Weighted latency [s]', False)
                continue
            elif "TOTAL" in statement:
            #    plot_statement(label_dfs_hash, statement, evaluation_result_column, statement, 'Weighted latency [s]', False)
                continue
            raise('statement not match' + statement)


def calculate_r2(label_dfs_hash):
    for label in label_dfs_hash.keys():
        print("=====================================")
        print(label)
        print("=====================================")
        for statement in label_dfs_hash[label].keys():
            actual = list(label_dfs_hash[label][statement][0]['mean'].values)
            estimated = list(label_dfs_hash[label][statement][0]['cost'].values)
            print(statement)
            print(r2_score(actual, estimated))


def plot_statement(dir_name, l_dfs_hash, statement, target_column, title, y_label, does_plot_se):
    label_data_hash = {}
    label_se_hash = {}
    for label in l_dfs_hash.keys():
        if statement in l_dfs_hash[label]:
            label_data_hash[label] = list(
                l_dfs_hash[label][statement][0][target_column].values.tolist())
            if does_plot_se and 'standard_error' in l_dfs_hash[label][statement][0].columns:
                label_se_hash[label] = list(
                    l_dfs_hash[label][statement][0]['standard_error'].values.tolist())
    Graph.plot_graph(dir_name, title, 'time step', y_label, label_data_hash, label_se_hash)


def sumup_each_series(max_ts, dataframes):
    tmp = [0] * (max_ts + 1)
    for df in dataframes:
        for t, m in df[['timestep', evaluation_result_column]].values.tolist():
            tmp[int(t)] += m

    return np.array([t if t is not 0 else np.nan for t in tmp])


def group_dfs_by_statement(dfs):
    statement_hash = {}
    for df in dfs:
        statement = list(
            map(lambda gn: gn[0] + "_" + gn[1], df[['group', 'name']].values.tolist()))[0]
        if statement in statement_hash:
            statement_hash[statement].append(df)
        else:
            statement_hash[statement] = [df]

        if statement.split('_')[1].startswith(
                "UPDATE") or statement.split('_')[1].startswith("INSERT"):
            statement = "Aggregated-" + statement.split(" -- ")[0]
            if statement in statement_hash:
                statement_hash[statement].append(df)
            else:
                statement_hash[statement] = [df]
    return statement_hash


def show_upseart_plan_num_each_ts(statement_dfs_hash, timestep):
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


def avg_query_latency(df):
    values = [[]
              for _ in range((max(df['timestep'].values.tolist()) + 1))]
    for ts, v in df.query("name.str.startswith(\"SELECT\")")[
        ['timestep', evaluation_result_column]].values.tolist():
        values[int(ts)].append(v)
    avg_values = [sum(vs) / len(vs) for vs in values]
    return avg_values


# count the number of statements in the group
# count INSERT into each cf as one original INSERT statement
def count_statement_num_for_each_ts(df, group_name):
    statement_counts = [[] for _ in range((max(df['timestep'].values.tolist()) + 1))]
    for ts in sorted(set(list(df.query("group == @group_name and name != \"TOTAL\"")['timestep'].values.tolist()))):
        statement_counts[int(ts)] = len(set(list(map(lambda x : x[0].split("for")[0], df.query("group == @group_name and name != \"TOTAL\" and timestep == @ts")[['name']].values.tolist()))))
    return statement_counts


def avg_group_latency(df, group_name):
    values = [[] for _ in range((max(df['timestep'].values.tolist()) + 1))]
    for ts, v in df.query("group == @group_name and name != \"TOTAL\"")[['timestep', evaluation_result_column]].values.tolist():
        values[int(ts)].append(v)

    statement_counts = count_statement_num_for_each_ts(df, group_name)
    avg_values = [sum(vs) / statement_counts[idx] for idx, vs in enumerate(values)]
    return avg_values


def weighted_avg_group_latency(df, group_name):
    weighted_group_totals = [[] for _ in range((max(df['timestep'].values.tolist()) + 1))]
    # TOTAL mean of each group is already weighted
    for ts, v in df.query("group == @group_name and name == \"TOTAL\"")[['timestep', evaluation_result_column]].values.tolist():
        if int(ts) in weighted_group_totals:
            raise Exception
        weighted_group_totals[int(ts)] = v

    statement_counts = count_statement_num_for_each_ts(df, group_name)
    avg_values = [t / statement_counts[idx] for idx, t in enumerate(weighted_group_totals)]
    return avg_values


def avg_upseart_latency(df, insert_statement_num):
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


def plot_unweighted_query_latency(dir_name, label_dfs_hash):
    label_data_hash = {}
    for label in label_grouped_dfs_hash.keys():
        label_data_hash[label] = avg_query_latency(label_dfs_hash[label])
    Graph.plot_graph(
        dir_name,
        "",
        "timestep",
        "Average Query Latency [s]",
        label_data_hash, {})


def plot_unweighted_group_latency(dir_name, label_dfs_hash):
    label_data_hash = {}

    groups = set()
    for label in label_grouped_dfs_hash.keys():
        groups |= set(label_dfs_hash[label].group.values)

    groups.remove("TOTAL")
    for g in groups:
        for label in label_grouped_dfs_hash.keys():
            label_data_hash[label] = avg_group_latency(label_dfs_hash[label], g)

        Graph.plot_graph(
            dir_name,
            "",
            "timestep",
            "Average latency of "+ g + " [s]",
            label_data_hash, {})


def plot_unweighted_upsert_latency(dir_name, label_dfs_hash, label_grouped_dfs_hash):
    label_data_hash = {}
    for label in label_grouped_dfs_hash.keys():
        insert_statement_nums = len([s for s in label_grouped_dfs_hash[label].keys() if "Aggregate" in s and ("INSERT" in s or "UPDATE" in s)])
        label_data_hash[label] = avg_upseart_latency(label_dfs_hash[label], insert_statement_nums)
    Graph.plot_graph(
        dir_name,
        "Average Insert Latency [s]",
        "timestep",
        "Average Insert Latency[s]",
        label_data_hash, {})


def show_upseart_plan_num(label_grouped_dfs_hash):
    for label in label_grouped_dfs_hash.keys():
        print("=" + label)
        show_upseart_plan_num_each_ts(
            label_grouped_dfs_hash[label], max_timestep)


def show_total_weighted_latency_diff(label_dfs_hash):
    print("TOTAL diff")
    label_total_weighted_avg_hash = get_total_weighted_avg_hash(label_dfs_hash)
    for label1 in label_total_weighted_avg_hash.keys():
        for label2 in label_total_weighted_avg_hash.keys():
            if label1 == label2:
                continue
            label1_total_weighted_latency = sum(label_total_weighted_avg_hash[label1])
            label2_total_weighted_latency = sum(label_total_weighted_avg_hash[label2])
            print("  " + label1 + ": " + str(label1_total_weighted_latency))
            print("  " + label2 + ": " + str(label2_total_weighted_latency))
            print(label1 + " / " + label2)
            print(str(label1_total_weighted_latency / label2_total_weighted_latency))
            print(" " + str((1 - label1_total_weighted_latency / label2_total_weighted_latency) * 100) + "% reduced")


def get_total_weighted_avg_hash(label_dfs_hash):
    label_total_weighted_avg_hash = {}

    groups = set()
    for label in label_grouped_dfs_hash.keys():
        groups |= set(label_dfs_hash[label].group.values)

    groups.remove("TOTAL")
    for label in label_grouped_dfs_hash.keys():
        for g in groups:
            if label in label_total_weighted_avg_hash:
                label_total_weighted_avg_hash[label] = \
                    [a + b for a, b in
                        zip(label_total_weighted_avg_hash[label], weighted_avg_group_latency(label_dfs_hash[label], g))]
            else:
                label_total_weighted_avg_hash[label] = weighted_avg_group_latency(label_dfs_hash[label], g)
    return label_total_weighted_avg_hash



def plot_weighted_total_latency(dir_name, label_dfs_hash):
    label_total_weighted_avg_hash = get_total_weighted_avg_hash(label_dfs_hash)

    #frequency_type = "Periodical"
    #frequency_type = "Linear"
    frequency_type = "Spike"

    Graph.plot_graph(
        dir_name,
        frequency_type + ": " + "Frequency weighted average latency [s]",
        "timestep",
        "Frequency weighted average latency [s]",
        label_total_weighted_avg_hash, {})


label_grouped_dfs_hash = {}
label_dfs_hash = {}
max_timestep = -1
evaluation_result_column = 'mean'
for i in range(1, len(sys.argv)):
    file_name = sys.argv[i]
    dataframe = FileLoader.file2dataframe(file_name)
    #dir_name = file_name.split('.')[0].split('/')[0]
    dir_name = "/".join(file_name.split('.')[0].split('/')[0:-1])
    label = file_name.split('.')[0].split('/')[-1]
    max_timestep = max(dataframe['timestep'].values.tolist())
    statement_dfs = FileLoader.file_2_statement_dfs(dataframe)

    label_dfs_hash[label] = dataframe
    label_grouped_dfs_hash[label] = group_dfs_by_statement(statement_dfs)

DIR_NAME = dir_name
plot_weighted_total_latency(dir_name, label_dfs_hash)
plot_unweighted_group_latency(dir_name, label_dfs_hash)
plot_unweighted_query_latency(dir_name, label_dfs_hash)
plot_unweighted_upsert_latency(dir_name, label_dfs_hash, label_grouped_dfs_hash)
plot_queries(dir_name, label_grouped_dfs_hash)
#calculate_r2(label_grouped_dfs_hash)
show_upseart_plan_num(label_grouped_dfs_hash)
show_total_weighted_latency_diff(label_dfs_hash)
