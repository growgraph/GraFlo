import argparse

from graph_cast.plot.plotter import SchemaPlotter

"""

graphviz attributes 

https://renenyffenegger.ch/notes/tools/Graphviz/attributes/index
https://rsms.me/graphviz/
https://graphviz.readthedocs.io/en/stable/examples.html
https://graphviz.org/doc/info/attrs.html

usage: 
    color='red',style='filled', fillcolor='blue',shape='square'

to keep 
level_one = [node1, node2]
sg_one = ag.add_subgraph(level_one, rank='same')

"""


def knapsack(weights, ks_size=7):
    """
    split a set of weights into bag (groups) of total weight of at most threshold weight
    :param weights:
    :param ks_size:
    :return:
    """
    pp = sorted(list(zip(range(len(weights)), weights)), key=lambda x: x[1])
    print(pp)
    acc = []
    if pp[-1][1] > ks_size:
        raise ValueError("One of the items is larger than the knapsack")

    while pp:
        w_item = []
        w_item += [pp.pop()]
        ww_item = sum([l for _, l in w_item])
        while ww_item < ks_size:
            cnt = 0
            for j, item in enumerate(pp[::-1]):
                diff = ks_size - item[1] - ww_item
                if diff >= 0:
                    cnt += 1
                    w_item += [pp.pop(len(pp) - j - 1)]
                    ww_item += w_item[-1][1]
                else:
                    break
            if ww_item >= ks_size or cnt == 0:
                acc += [w_item]
                break
    acc_ret = [[y for y, _ in subitem] for subitem in acc]
    return acc_ret


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c", "--config-path", required=True, help="path to config file"
    )

    parser.add_argument(
        "-f",
        "--figure-output-path",
        required=True,
        help="path to output the figure",
    )
    parser.add_argument(
        "-p",
        "--prune-low-degree-nodes",
        action="store_true",
        help="prune low degree nodes for vc2vc",
    )

    args = parser.parse_args()

    plotter = SchemaPlotter(args.config_path, args.figure_output_path)
    plotter.plot_vc2fields()
    plotter.plot_source2vc()
    plotter.plot_vc2vc(prune_leaves=args.prune_low_degree_nodes)
    # plotter.plot_source2vc_detailed()


if __name__ == "__main__":
    main()
