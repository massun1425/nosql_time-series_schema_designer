import sys

import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import pyparsing as pp
import numpy as np

def change_file_name(name):
    name_hash = {
        # ファイル名以外の名前に設定する場合は以下にファイル名と表示名の対応関係を記入してください
        "cyclic_prop_80per_15230144000_4ts.txt": "prop_4ts",
        "cyclic_prop_80per_no_iterative_15230144000_4ts.txt": "prop_no_pruning_4ts",
        "cyclic_prop_80per_15230144000_8ts.txt": "prop_8ts",
        "cyclic_prop_80per_no_iterative_15230144000_8ts.txt": "prop_no_pruning_8ts",
        "cyclic_prop_80per_15230144000_12ts.txt": "prop_12ts",
        "cyclic_prop_80per_no_iterative_15230144000_12ts.txt": "prop_no_pruning_12ts",
        "cyclic_prop_80per_15230144000_16ts.txt": "prop_16ts",
        "cyclic_prop_80per_no_iterative_15230144000_16ts.txt": "prop_no_pruning_16ts",
        "cyclic_prop_80per_15230144000_20ts.txt": "prop_20ts",
        "cyclic_prop_80per_no_iterative_15230144000_20ts.txt": "prop_no_pruning_20ts",
        "cyclic_prop_80per_15230144000_24ts.txt": "prop_24ts",
        "cyclic_prop_80per_no_iterative_15230144000_24ts.txt": "prop_no_pruning_24ts",
        "cyclic_prop_80per_15230144000_28ts.txt": "prop_28ts",
        "cyclic_prop_80per_no_iterative_15230144000_28ts.txt": "prop_no_pruning_28ts",
        "cyclic_prop_80per_15230144000_32ts.txt": "prop_32ts",
        "cyclic_prop_80per_no_iterative_15230144000_32ts.txt": "prop_no_pruning_32ts",
        "cyclic_prop_80per_15230144000_36ts.txt": "prop_36ts",
        "cyclic_prop_80per_no_iterative_15230144000_36ts.txt": "prop_no_pruning_36ts",
        "cyclic_prop_80per_15230144000_40ts.txt": "prop_40ts",
        "cyclic_prop_80per_no_iterative_15230144000_40ts.txt": "prop_no_pruning_40ts",
        "cyclic_prop_80per_15230144000_44ts.txt": "prop_44ts",
        "cyclic_prop_80per_no_iterative_15230144000_44ts.txt": "prop_no_pruning_44ts",
    }
    if name not in name_hash:
        return name
    return name_hash[name]


file_dataframe = {}
for i in range(1, len(sys.argv)):
    file_name = sys.argv[i]
    with open(file_name) as f:
        tmp_lines = f.readlines()
    is_running_time_record = False
    running_time_record = []
    for t in tmp_lines:
        if t == "</running time log> ===========================\n":
            is_running_time_record = False
        if is_running_time_record:
            running_time_record.append(t)
        if t == "<running time log> ===========================\n":
            is_running_time_record = True
    print(file_name)
    columns = pp.commaSeparatedList.parseString(running_time_record[0]).asList()
    values = list(map(lambda x: int(x) if x != '' else 0, pp.commaSeparatedList.parseString(running_time_record[1]).asList()))
    data_name = change_file_name(file_name.split("/")[-1])
    file_dataframe[data_name] = pd.DataFrame([values], columns=columns)

print(file_dataframe)


run_columns = ["CF_ENUMERATION", "PLAN_ENUMERATION", "MIGPLAN_ENUMERATION", "PRUNING", "OPTIMIZATION", "OTHER"]
run_dfs = {}
run_values = []
file_names = []

for file_name, df in file_dataframe.items():
    run_df = {}
    enumeration = df['END_CF_ENUMERATION'].values[0] - df['START_CF_ENUMERATION'].values[0]
    run_df['CF_ENUMERATION'] = enumeration
    run_df['PLAN_ENUMERATION'] =  df['END_QUERY_PLAN_ENUMERATION'].values[0] - df['START_QUERY_PLAN_ENUMERATION'].values[0]
    run_df['MIGPLAN_ENUMERATION'] = df['END_MIGRATION_PLAN_ENUMERATION'].values[0] - df['START_MIGRATION_PLAN_ENUMERATION'].values[0]
    run_df['PRUNING'] =  df['END_PRUNING'].values[0] - df['START_PRUNING'].values[0]
    run_df['OPTIMIZATION'] = df['END_WHOLE_OPTIMIZATION'].values[0] - df['START_WHOLE_OPTIMIZATION'].values[0]
    run_df['OTHER'] = df['END'].values[0] - df['START'].values[0] - sum(run_df.values())

    #run_dfs[file_name] = run_dataframe

    print(run_df)

    run_df_sec = {}
    for k, v in run_df.items():
        run_df_sec[k] = v / 1000.0

    current_values = []
    for c in run_columns:
        current_values.append(run_df_sec[c])
    run_values.append(current_values)
    file_names.append(file_name.split('.')[0].split('/')[-1])

run_values_t = np.array(run_values).T.tolist()

run_dataframe = pd.DataFrame(run_values_t, index=run_columns, columns=file_names)

#fig, ax = plt.subplots(figsize = (10, 0))
#for i in range(len(run_dataframe)):
#    ax.bar(run_dataframe.columns, run_dataframe.iloc[i], bottom=run_dataframe.iloc[:i].sum())
#ax.set(xlabel=file_name, ylabel='running time')
#ax.legend(run_dataframe.index)
#plt.savefig('tmp.jpg')

plt.rcParams["font.size"] = 17
plt.rcParams['figure.subplot.bottom'] = 0.15
fig = plt.figure(figsize=(10, 3))
ax = fig.add_subplot(1, 1, 1)
for i in range(len(run_dataframe)):
    ax.bar(run_dataframe.columns, run_dataframe.iloc[i], bottom=run_dataframe.iloc[:i].sum(), linewidth=50)
ax.set(xlabel="workloads with various number of time step", ylabel='Running Time[s]')
ax.legend(run_dataframe.index)
ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
#plt.title("running time [s]")
plt.xticks(rotation=85)
#plt.yscale("log")
plt.subplots_adjust(left=0.2, right=0.95, bottom=0.53, top=0.9)
#plt.savefig('tmp.jpg')
plt.show()


