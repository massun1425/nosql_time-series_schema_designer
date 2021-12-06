from pathlib import Path
import matplotlib.pyplot as pyplot
import numpy as np


class Graph:
    @classmethod
    def convert_legends(cls, legend):
        convert_hash = {
            "cyclic_prop_80per_15230144000_12ts": "prop.",
            "cyclic_prop_80per_no_iterative_15230144000_12ts": "prop. without cf pruning",
            "cyclic_first_80per_15230144000_12ts": "first time step freq.",
            "cyclic_last_80per_15230144000_12ts": "last time step freq.",
            "cyclic_static_80per_15230144000_12ts": "average freq.",
            "cyclic_ideal_80per_15230144000_12ts": "ideal optimization",

            "monotonic_prop_80per_15230144000_12ts": "prop.",
            "monotonic_prop_80per_no_iterative_15230144000_12ts": "prop. without cf pruning",
            "monotonic_first_80per_15230144000_12ts": "first time step freq.",
            "monotonic_last_80per_15230144000_12ts": "last time step freq.",
            "monotonic_static_80per_15230144000_12ts": "average freq.",
            "monotonic_ideal_80per_15230144000_12ts": "ideal optimization",

            "peak_prop_80per_15230144000_12ts": "prop.",
            "peak_prop_80per_no_iterative_15230144000_12ts": "prop. without cf pruning",
            "peak_first_80per_15230144000_12ts": "first time step freq.",
            "peak_last_80per_15230144000_12ts": "last time step freq.",
            "peak_static_80per_15230144000_12ts": "average freq.",
            "peak_ideal_80per_15230144000_12ts": "ideal optimization"
        }
        if legend in convert_hash:
            return convert_hash[legend]
        return legend

    @classmethod
    def plot_graph(cls, dir_name, title, x_label, y_label, label_data_hash, label_se_hash):
        x = list(range(0, len(list(label_data_hash.values())[0])))

        # fig = pyplot.figure(dpi=300)
        fig = pyplot.figure(figsize=(7, 4))
        ax = fig.add_subplot(1, 1, 1)
        y_max_lim = 0
        cmap = pyplot.get_cmap("tab10")
        makers = ["o", "v", "^", "<", ">", "1", "2", "3"]
        order = 100
        for idx, label in enumerate(label_data_hash.keys()):
            if label_data_hash[label] is not None:
                ax.plot(x, label_data_hash[label], marker=makers[idx], label=Graph.convert_legends(label), linewidth=1.2, markersize=4, color=cmap(idx), zorder=order)
                order -= 1
                if y_max_lim < max(label_data_hash[label]):
                    y_max_lim = max(label_data_hash[label])
                # if label in label_cost_hash:
                # ax.plot(x, label_cost_hash[label], label=label + "_cost", marker="x")
                if bool(label_se_hash) and not any([np.isnan(se) for se in label_se_hash[label]]):
                    doubled_se_interval = [se * 2 for se in label_se_hash[label]]
                    ax.errorbar(x, label_data_hash[label], doubled_se_interval, fmt='o', capsize=2, ecolor=cmap(idx), markeredgecolor = cmap(idx), color=cmap(idx))

        #pyplot.rcParams["font.size"] = 13
        pyplot.title(Graph.title_with_newline(title))
        pyplot.subplots_adjust(left=0.2, right=0.95, bottom=0.13, top=0.9)
        pyplot.xlabel(x_label)
        pyplot.ylabel(y_label)

        pyplot.tick_params(labelsize=13)
        ax.set_ylim(ymin=0)
        ax.set_ylim(ymax=y_max_lim * 1.05)
        ax.set_xlim(xmin=0)
        ax.set_xlim(xmax=max(x))
        pyplot.legend(fontsize=9, ncol=2)
        #pyplot.legend(bbox_to_anchor=(0, -0.25), loc='upper left', borderaxespad=0, fontsize=8)
        output_dir = dir_name + "/figs/"
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
