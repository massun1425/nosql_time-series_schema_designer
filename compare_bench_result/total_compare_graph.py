import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as pyplot
import pyparsing as pp
import japanize_matplotlib
import numpy as np

# 総実行時間
# 1. 各時刻の各処理の応答時間に実行頻度を掛け合わせる (各処理の加重した応答時間)
# 2. 各グループごとに、グループ内の処理の加重した応答時間の平均値をとる
# 3. 時刻ごとにグループの加重した応答時間の平均値を足し合わせる
# 4. 全ての時刻においてこの値を足し合わせる。


class FileLoader:
    @classmethod
    def file2dataframe(cls, file_path):
        with open(file_path) as f:
            tmp_lines = f.readlines()
        column = pp.commaSeparatedList.parseString(tmp_lines[0]).asList()
        lines = []
        header = 'timestep,label,group,name,weight,mean,cost,standard_error\n'

        for tl in tmp_lines[1:]:
            if tl != header:
                tmp = pp.commaSeparatedList.parseString(tl).asList()
                tmp[3] = tmp[3].strip("\"")
                tmp[4] = tmp[4].strip("\"")
                lines.append(tmp)
        df = pd.DataFrame(lines, columns=column)
        df['timestep'] = df['timestep'].astype('int')
        df['mean'] = df['mean'].astype('float64')
        df['cost'] = df.cost.apply(lambda  x: float(x) if x else np.nan)
        if 'standard_error' in column:
            df['standard_error'] = df['standard_error'].astype('float64')
        return df

    @classmethod
    def file_2_statement_dfs(cls, df):
        statements = []
        for g, n in df[['group', 'name']].values:
            statements.append(g + n)
        statements = set(statements)
        stmnt_dfs = []
        for s in statements:
            stmnt_dfs.append(
                df[df['group'] + df['name'] == s])
        return stmnt_dfs


class Graph:
    @classmethod
    def convert_legends(cls, legend):
        convert_hash = {
            "bench_tpch_22q_prop": "提案手法",
            "bench_tpch_22q_static": "平均実行頻度に対する最適化",
            "bench_tpch_22q_first": "開始時実行頻度に対する最適化",
            "bench_tpch_22q_last": "終了時実行頻度に対する最適化",

            "simple_prop": "提案手法",
            "simple_static": "平均実行頻度に対する最適化",
            "simple_first": "開始時実行頻度に対する最適化",
            "simple_last": "終了時実行頻度に対する最適化",

            "bench_90per_8008326000_prop": "提案手法",
            "bench_90per_8008326000_static": "平均実行頻度に対する最適化",
            "bench_90per_8008326000_first": "開始時実行頻度に対する最適化",
            "bench_90per_8008326000_last": "終了時実行頻度に対する最適化",

            "bench_non_iterative_first": "開始時頻度に対する最適化",
            "bench_non_iterative_last": "終了時頻度に対する最適化",
            #"bench_non_iterative_first": "開始時実行頻度に対する最適化",
            #"bench_non_iterative_last": "終了時実行頻度に対する最適化",
            #"bench_non_iterative_static": "平均実行頻度に対する最適化",
          "bench_non_iterative_static": "平均頻度に対する最適化",
            #"bench_non_iterative_prop": "提案手法 (候補削減無し)" ,
            #"bench_iterative_prop": "提案手法 (候補削減有り)" ,
            "bench_iterative_prop": "提案手法",

        }
        if legend in convert_hash:
            return convert_hash[legend]
        return legend

    #@classmethod
    #def convert_labels(cls, label):
    #    convert_hash = {
    #        "Average Latency [s]": "平均応答時間 [s]",
    #        "Weighted Latency [s]": "応答時間の各処理の実行頻度による加重平均値 [s]",
    #        "timestep": "時刻"
    #    }
    #    if label in convert_hash:
    #        return convert_hash[label]
    #    return label


    @classmethod
    def plot_graph(cls, title, x_label, y_label, label_data_hash, label_se_hash):
        x = list(range(0, len(list(label_data_hash.values())[0])))

        # fig = pyplot.figure(dpi=300)
        #fig = pyplot.figure(dpi=200, figsize=(80, 60))
        fig = pyplot.figure(figsize=(8, 4.5))
        #fig = pyplot.figure(figsize=(8, 4))
        ax = fig.add_subplot(1, 1, 1)
        y_max_lim = 0
        cmap = pyplot.get_cmap("tab10")
        makers = ["o", "v", "^", "<", ">", "1", "2", "3"]
        for idx, label in enumerate(label_data_hash.keys()):
            if label_data_hash[label] is not None:
                if idx == 1:
                  idx = 2
                elif idx == 2:
                  idx = 1
                ax.plot(x, label_data_hash[label], marker=makers[idx], label=Graph.convert_legends(label), linewidth=1.5, markersize=4,color=cmap(idx) )
                if y_max_lim < max(label_data_hash[label]):
                    y_max_lim = max(label_data_hash[label])
                # if label in label_cost_hash:
                # ax.plot(x, label_cost_hash[label], label=label + "_cost", marker="x")
                if bool(label_se_hash) and not any([np.isnan(se) for se in label_se_hash[label]]):
                    doubled_se_interval = [se * 2 for se in label_se_hash[label]]
                    ax.errorbar(x, label_data_hash[label], doubled_se_interval, fmt='o', capsize=2, ecolor='black', markeredgecolor = "black", color='w')

        pyplot.rcParams["font.size"] = 13
        pyplot.title(Graph.title_with_newline(title))
        #pyplot.legend(fontsize=13)
        pyplot.subplots_adjust(left=0.2, right=0.95, bottom=0.13, top=0.95)
        #x_label = Graph.convert_labels(x_label)
        #y_label = Graph.convert_labels(y_label)
        pyplot.xlabel(x_label, fontsize=14)
        pyplot.ylabel(y_label, fontsize=13)
        pyplot.tick_params(labelsize=13)
        ax.set_ylim(ymin=0)
        ax.set_ylim(ymax=y_max_lim * 1.1)
        ax.set_xlim(xmin=0)
        ax.set_xlim(xmax=max(x))
        pyplot.legend()
        output_dir= DIR_NAME + "/figs/"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        fig.savefig(output_dir + title.split('--')[-1].strip(" ") + "_" + y_label.strip(" ") + ".pdf")
        #fig.show()

    @classmethod
    def title_with_newline(cls, title):
        tmp = ""
        for t in title:
            tmp += t
            if len(tmp) % 90 == 0:
                tmp += "\n"
        return tmp


def plot_queries(
        max_ts,
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
                plot_statement(label_dfs_hash, statement, 'mean', statement.split("--")[1], 'Latency [s]', True)
                plot_statement(label_dfs_hash, statement, 'cost', "COST" + "\n" + statement, 'Estimated Cost', False)
                continue
            elif "TOTAL_TOTAL" == statement:
                plot_statement(label_dfs_hash, statement, 'mean',"Frequency-weighted Total Latency [s]", 'Weighted latency [s]', False)
                continue
            elif "TOTAL" in statement:
                plot_statement(label_dfs_hash, statement, 'mean', statement, 'Weighted latency [s]', False)
                continue
            raise('statement not match' + statement)


def plot_statement(l_dfs_hash, statement, target_column, title, y_label, does_plot_se):
    label_data_hash = {}
    label_se_hash = {}
    for label in l_dfs_hash.keys():
        if statement in l_dfs_hash[label]:
            label_data_hash[label] = list(
                l_dfs_hash[label][statement][0][target_column].values.tolist())
            if does_plot_se and 'standard_error' in l_dfs_hash[label][statement][0].columns:
                label_se_hash[label] = list(
                    l_dfs_hash[label][statement][0]['standard_error'].values.tolist())
    Graph.plot_graph(title, 'timestep', y_label, label_data_hash, label_se_hash)


def sumup_each_series(max_ts, dataframes):
    tmp = [0] * (max_ts + 1)
    for df in dataframes:
        for t, m in df[['timestep', 'mean']].values.tolist():
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
        ['timestep', 'mean']].values.tolist():
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
    for ts, v in df.query("group == @group_name and name != \"TOTAL\"")[['timestep', 'mean']].values.tolist():
        values[int(ts)].append(v)

    statement_counts = count_statement_num_for_each_ts(df, group_name)
    avg_values = [sum(vs) / statement_counts[idx] for idx, vs in enumerate(values)]
    return avg_values


def weighted_avg_group_latency(df, group_name):
    weighted_group_totals = [[] for _ in range((max(df['timestep'].values.tolist()) + 1))]
    # TOTAL mean of each group is already weighted
    for ts, v in df.query("group == @group_name and name == \"TOTAL\"")[['timestep', 'mean']].values.tolist():
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
        ['timestep', 'mean']].values.tolist():
        values[int(ts)].append(v)
    for ts, v in df.query("name.str.startswith(\"INSERT\")")[
        ['timestep', 'mean']].values.tolist():
        values[int(ts)].append(v)
    if insert_statement_num == 0:
        return [0]
    avg_values = [sum(vs) / insert_statement_num for vs in values]
    return avg_values


def plot_unweighted_query_latency(label_dfs_hash):
    label_data_hash = {}
    for label in label_grouped_dfs_hash.keys():
        label_data_hash[label] = avg_query_latency(label_dfs_hash[label])
    Graph.plot_graph(
        #"Average Query Latency [s]",
        "",
        "timestep",
        "Average Query Latency [s]",
        label_data_hash, {})


def plot_unweighted_group_latency(label_dfs_hash):
    label_data_hash = {}

    groups = set()
    for label in label_grouped_dfs_hash.keys():
        groups |= set(label_dfs_hash[label].group.values)

    groups.remove("TOTAL")
    for g in groups:
        for label in label_grouped_dfs_hash.keys():
            label_data_hash[label] = avg_group_latency(label_dfs_hash[label], g)

        if g == "Even":
            g = "偶数グループ"
        elif g == "Odd":
            g = "奇数グループ"

        if g == "Test1":
            g = "グループ1"
        elif g == "Test2":
            g = "グループ2"

        Graph.plot_graph(
            #g + "の平均応答時間 [s]",
            "",
            "時刻",
            g + "の平均応答時間 [s]",
            label_data_hash, {})

        #Graph.plot_graph(
        #   #"Average Latency of " + g + " group [s]",
        #   t,
        #   "時刻",
        #   "平均応答時間 [s]",
        #   label_data_hash, {})



def plot_unweighted_upsert_latency(label_dfs_hash, label_grouped_dfs_hash):
    label_data_hash = {}
    for label in label_grouped_dfs_hash.keys():
        insert_statement_nums = len([s for s in label_grouped_dfs_hash[label].keys() if "Aggregate" in s and ("INSERT" in s or "UPDATE" in s)])
        label_data_hash[label] = avg_upseart_latency(label_dfs_hash[label], insert_statement_nums)
    Graph.plot_graph(
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



def plot_weighted_total_latency(label_dfs_hash):
    label_total_weighted_avg_hash = get_total_weighted_avg_hash(label_dfs_hash)

    #Graph.plot_graph(
    #    "Frequency weighted average latency [s]",
    #    "timestep",
    #    "Frequency weighted average latency [s]",
    #    label_total_weighted_avg_hash, {})
    Graph.plot_graph(
        #"各処理の実行頻度による応答時間の加重平均 [s]",
        "",
        "時刻",
        "各処理の応答時間の実行頻度による加重平均 [s]",
        #"応答時間の実行頻度による加重平均 [s]",
        label_total_weighted_avg_hash, {})


label_grouped_dfs_hash = {}
label_dfs_hash = {}
max_timestep = -1
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
plot_weighted_total_latency(label_dfs_hash)
plot_unweighted_group_latency(label_dfs_hash)
plot_unweighted_query_latency(label_dfs_hash)
plot_unweighted_upsert_latency(label_dfs_hash, label_grouped_dfs_hash)
plot_queries(max_timestep, label_grouped_dfs_hash, )
show_upseart_plan_num(label_grouped_dfs_hash)
show_total_weighted_latency_diff(label_dfs_hash)
