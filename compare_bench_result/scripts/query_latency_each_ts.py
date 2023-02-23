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

        }
        if legend in convert_hash:
            return convert_hash[legend]
        return legend

    @classmethod
    def plot_graph(cls, title, x_label, y_label, label_data_hash, label_se_hash):
        x = list(range(0, len(list(label_data_hash.values())[0])))

        # fig = pyplot.figure(dpi=300)
        #fig = pyplot.figure(dpi=200, figsize=(80, 60))
        fig = pyplot.figure(figsize=(8, 5))
        ax = fig.add_subplot(1, 1, 1)
        y_max_lim = 0
        makers = ["o", "v", "^", "<", ">", "1", "2", "3"]
        for idx, label in enumerate(label_data_hash.keys()):
            if label_data_hash[label] is not None:
                ax.plot(x, label_data_hash[label], marker=makers[idx], label=Graph.convert_legends(label), linewidth=1.5, markersize=4)
                if y_max_lim < max(label_data_hash[label]):
                    y_max_lim = max(label_data_hash[label])
                # if label in label_cost_hash:
                # ax.plot(x, label_cost_hash[label], label=label + "_cost", marker="x")
                if bool(label_se_hash) and not any([np.isnan(se) for se in label_se_hash[label]]):
                    doubled_se_interval = [se * 2 for se in label_se_hash[label]]
                    #ax.errorbar(x, label_data_hash[label], doubled_se_interval, fmt='o', capsize=2, ecolor='black', markeredgecolor = "black", color='w')

        #pyplot.rcParams["font.size"] = 12
        pyplot.rcParams["font.size"] = 20 # resume
        pyplot.title(Graph.title_with_newline(title), fontsize=11)
        pyplot.legend(fontsize=12)
        #x_label = Graph.convert_labels(x_label)
        #y_label = Graph.convert_labels(y_label)
        pyplot.xlabel(x_label, fontsize=20)
        pyplot.ylabel(y_label, fontsize=20)
        ax.set_ylim(ymin=0)
        ax.set_ylim(ymax=y_max_lim * 1.1)
        ax.set_xlim(xmin=0)
        ax.set_xlim(xmax=max(x))
        pyplot.legend()
        output_dir= DIR_NAME + "/figs/"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        #fig.savefig(output_dir + title.split('--')[-1].strip(" ") + "_" + y_label.strip(" ") + ".pdf")
        fig.show()

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
        if statement.split('_')[1].startswith("SELECT"):
            plot_statement(label_dfs_hash, statement, 'mean', statement.split("--")[1], 'Latency [s]', True)


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


def plot_query_latencies(max_timestep, statement_dfs):
    label_data_hash = {}
    for s_dfs in statement_dfs:
        if not s_dfs.name in label_data_hash:
            label_data_hash[s_dfs.name] = {}
        for ts, m in s_dfs[['timestep', 'mean']]:
            label_data_hash[s_dfs.name][ts] = m
    label_data_hash




label_grouped_dfs_hash = {}
label_dfs_hash = {}
max_timestep = -1
file_name = sys.argv[1]
dataframe = FileLoader.file2dataframe(file_name)
#dir_name = file_name.split('.')[0].split('/')[0]
dir_name = "/".join(file_name.split('.')[0].split('/')[0:-1])
label = file_name.split('.')[0].split('/')[-1]
max_timestep = max(dataframe['timestep'].values.tolist())
statement_dfs = FileLoader.file_2_statement_dfs(dataframe)

DIR_NAME = dir_name
#plot_queries(max_timestep, label_grouped_dfs_hash, )
plot_query_latencies(max_timestep, statement_dfs)
