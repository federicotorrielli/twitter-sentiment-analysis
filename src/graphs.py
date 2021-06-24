import matplotlib.pyplot as plt


def build_histogram_matplotlib(data: dict, xlabel_name: str):
    """
    Build a histogram using matplotlib
    @param data: a dict {'label': value, ...}
    @param xlabel_name: x unit
    """
    group_data = list(data.values())
    group_names = list(data.keys())
    fig, ax = plt.subplots()
    ax.barh(group_names, group_data, label='Men')
    ax.set_xlabel(xlabel_name)
    fig.show()
