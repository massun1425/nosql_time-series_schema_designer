import sys
import pandas as pd
import matplotlib.pyplot as pyplot
import pyparsing as pp
import japanize_matplotlib
import numpy as np


def file2dataframe(file_path):
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
    df['standard_error'] = df['standard_error'].astype('float64')
    return df


def file_2_statement_dfs(df):
    statements = []
    for g, n in df[['group', 'name']].values:
        statements.append(g + n)
    statements = set(statements)
    statement_dfs = []
    for s in statements:
        statement_dfs.append(
            df[df['group'] + df['name'] == s])
    return statement_dfs


def plot_graph(title, x_label, y_label, label_data_hash, label_se_hash):
    x = list(range(0, len(list(label_data_hash.values())[0])))

    fig = pyplot.figure(dpi=300)
    ax = fig.add_subplot(1, 1, 1)
    for label in label_data_hash.keys():
        if label_data_hash[label] is not None:
            ax.plot(x, label_data_hash[label], label=label, marker="o")
            if label_se_hash is not None and not any([np.isnan(se) for se in label_se_hash[label]]):
                doubled_se_interval = [se * 2 for se in label_se_hash[label]]
                ax.errorbar(x, label_data_hash[label], doubled_se_interval, fmt='ro', capsize=4, ecolor='black')

    pyplot.title(title_with_newline(title), fontsize=10)
    pyplot.xlabel(x_label, fontsize=10)
    pyplot.ylabel(y_label, fontsize=10)
    ax.set_ylim(ymin=0)
    pyplot.legend()
    fig.show()


def title_with_newline(title):
    tmp = ""
    for t in title:
        tmp += t
        if len(tmp) % 70 == 0:
            tmp += "\n"
    return tmp


def plot_queries(
        max_ts,
        label_dfs_hash):
    for statement in list(label_dfs_hash.values())[0].keys():
        if statement.split('_')[1].startswith(
                "UPDATE") or statement.split('_')[1].startswith("INSERT"):
            label_data_hash = {}
            label_se_hash = {}
            for label in label_dfs_hash.keys():
                if statement in label_dfs_hash[label]:
                    related_dfs1 = label_dfs_hash[label][statement]
                    label_data_hash[label] = sumup_each_series(
                        max_ts, related_dfs1)
                    label_se_hash[label] = list(
                      label_dfs_hash[label][statement][0]['standard_error'].values.tolist())
            plot_graph(statement, 'timesteps', 'Latency [s]', label_data_hash, label_se_hash)
        else:
            if statement.split('_')[1].startswith("SELECT"):
                title = statement
                y_label = 'Latency [s]'
            elif "TOTAL_TOTAL" == statement:
                title = "Frequency-weighted Average Latency [s]"
                y_label = 'Weighted latency [s]'
            elif "TOTAL" in statement:
                title = statement
                y_label = 'Weighted latency [s]'
            else:
                raise('statement not match' + statement)

            label_data_hash = {}
            label_se_hash = {}
            for label in label_dfs_hash.keys():
                if statement in label_dfs_hash[label]:
                    label_data_hash[label] = list(
                        label_dfs_hash[label][statement][0]['mean'].values.tolist())
                    label_se_hash[label] = list(
                      label_dfs_hash[label][statement][0]['standard_error'].values.tolist())
            plot_graph(title, 'timesteps', y_label, label_data_hash, label_se_hash)


def sumup_each_series(max_ts, dataframes):
    tmp = [0] * (max_ts + 1)
    for df in dataframes:
        for t, m in df[['timestep', 'mean']].values.tolist():
            tmp[int(t)] += m
    return tmp


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


def avg_query_latency(dataframe):
    values = [[]
              for _ in range((max(dataframe['timestep'].values.tolist()) + 1))]
    for ts, v in dataframe.query("name.str.startswith(\"SELECT\")")[
            ['timestep', 'mean']].values.tolist():
        values[int(ts)].append(v)
    avg_values = [sum(vs) / len(vs) for vs in values]
    return avg_values


def avg_upseart_latency(dataframe, insert_statement_num):
    values = [[]
              for _ in range((max(dataframe['timestep'].values.tolist()) + 1))]
    for ts, v in dataframe.query("name.str.startswith(\"UPDATE\")")[
            ['timestep', 'mean']].values.tolist():
        values[int(ts)].append(v)
    for ts, v in dataframe.query("name.str.startswith(\"INSERT\")")[
            ['timestep', 'mean']].values.tolist():
        values[int(ts)].append(v)
    avg_values = [sum(vs) / insert_statement_num for vs in values]
    return avg_values


def get_total_weighted_latency(dataframe):
    return sum(dataframe.query("group == \"TOTAL\" and name == \"TOTAL\"")[
               'mean'].values.tolist())


def show_unweighted_query_latency(label_dfs_hash):
    label_data_hash = {}
    for label in label_grouped_dfs_hash.keys():
        label_data_hash[label] = avg_query_latency(label_dfs_hash[label])
    plot_graph(
        "Average Query Latency [s]",
        "timestep",
        "Average Query Latency [s]",
        label_data_hash, None)


def show_unweighted_upsert_latency(label_dfs_hash, label_grouped_dfs_hash):
    label_data_hash = {}
    for label in label_grouped_dfs_hash.keys():
        insert_statement_nums = len([s for s in label_grouped_dfs_hash[label].keys() if "Aggregate" in s and ("INSERT" in s or "UPDATE" in s)])
        label_data_hash[label] = avg_upseart_latency(label_dfs_hash[label], insert_statement_nums)
    plot_graph(
        "Average Insert Latency [s]",
        "timestep",
        "Average Insert Latency[s]",
        label_data_hash, None)


def show_upseart_plan_num(label_grouped_dfs_hash):
    for label in label_grouped_dfs_hash.keys():
        print("=" + label)
        show_upseart_plan_num_each_ts(
            label_grouped_dfs_hash[label], max_timestep)


def show_total_weighted_latency_diff(label_dfs_hash):
    print("TOTAL diff")
    for label1 in label_dfs_hash.keys():
        for label2 in label_dfs_hash.keys():
            if label1 == label2:
                continue
            print(label1 + " / " + label2)
            print(" " +
                  str(get_total_weighted_latency(
                    label_dfs_hash[label1]) /
                get_total_weighted_latency(
                    label_dfs_hash[label2])))


label_grouped_dfs_hash = {}
label_dfs_hash = {}
max_timestep = -1
for i in range(1, len(sys.argv)):
    file_name = sys.argv[i]
    dataframe = file2dataframe(file_name)
    label = file_name.split('.')[0].split('/')[-1]
    max_timestep = max(dataframe['timestep'].values.tolist())
    statement_dfs = file_2_statement_dfs(dataframe)

    label_dfs_hash[label] = dataframe
    label_grouped_dfs_hash[label] = group_dfs_by_statement(statement_dfs)

show_unweighted_query_latency(label_dfs_hash)
show_unweighted_upsert_latency(label_dfs_hash, label_grouped_dfs_hash)
plot_queries(max_timestep, label_grouped_dfs_hash)
show_upseart_plan_num(label_grouped_dfs_hash)
show_total_weighted_latency_diff(label_dfs_hash)
