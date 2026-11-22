"""依赖图连通分量：决定一份租约必须覆盖哪些分区。

整链归一个持有者最安全，却让无关分区一起排队；各组分别持有虽能并行，
又可能把相互依赖的分区拆给不同人。这里的折中是：以无向连通分量为
持有边界——有依赖路径相连的数据集（含跨日滞后边）永远同属一个租约，
互不相连的分量可以由不同值班组并行持有。
"""


def undirected_edges(datasets):
    """从依赖注册表抽取无向边 (a, b)，去重、排序。"""
    edges = set()
    for child, record in datasets.items():
        for parent, _lag in record["reads"]:
            edges.add(tuple(sorted((child, parent))))
    return sorted(edges)


def components(nodes, edges):
    """返回 {数据集: 分量标识}，标识为分量内最小数据集名。"""
    parent = {name: name for name in nodes}

    def find(name):
        while parent[name] != name:
            parent[name] = parent[parent[name]]
            name = parent[name]
        return name

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            keep, drop = sorted((ra, rb))
            parent[drop] = keep

    for a, b in edges:
        if a in parent and b in parent:
            union(a, b)
    return {name: find(name) for name in nodes}


def dataset_of(partition):
    return partition.split("@", 1)[0]


def closure(partitions, datasets):
    """给定分区集合，返回必须一起持有的分区集合。

    先定位这些分区所属数据集的连通分量，再把同一分量内、与请求分区
    同分区日窗口相关的全部数据集分区纳入。跨日滞后意味着同一数据集的
    不同分区日也可能被同一条重放链触及，因此按数据集闭包 + 原分区日
    集合扩张。
    """
    names = {dataset_of(part) for part in partitions}
    nodes = set(datasets)
    membership = components(nodes, undirected_edges(datasets))
    roots = {membership[name] for name in names if name in membership}
    related_datasets = {name for name, root in membership.items() if root in roots}
    days = {part.split("@", 1)[1] for part in partitions}
    scope = set(partitions)
    for name in related_datasets:
        for day in days:
            scope.add(f"{name}@{day}")
    return sorted(scope), sorted(related_datasets)


def cluster_map(datasets):
    """整图的数据集 → 分量标识，用于展示与校验。"""
    return components(set(datasets), undirected_edges(datasets))
