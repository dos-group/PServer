import numpy
from sklearn.cluster import KMeans


# splits X into n equally sized partitions
def create_partitions(X, n):
    N = X.shape[0]
    partition_size = int(N/n)
    partitioning = [i*partition_size for i in range(n)]+[N]
    partitions = []
    for i, j in zip(partitioning[:-1], partitioning[1:]):
        partitions.append(X[i:j])
    return partitions


def repartition(n, old_partitions, gradients):
    km = KMeans(n)
    km.fit(gradients)

    partitions = [[] for i in range(n)]
    for i, l in enumerate(km.labels_):
        partitions[l].append(i)

    for i in range(n):
        numpy.random.shuffle(partitions[i])
    return partitions


def partition_label_matching(n, partitions, labels):
    ret = ""
    for i_partition, partition in enumerate(partitions):
        cnt = [0, 0]
        for i in partition:
            if labels[i] == -1:
                cnt[0] += 1
            elif labels[i] == +1:
                cnt[1] += 1
            else:
                assert False
        cnt_total = cnt[0]+cnt[1]
        ret += (
            "partition %2i of %5i elem matches"
            " -1: %5i(%2.1f), +1: %5i(%.2f)\n" % (
                i_partition, cnt_total,
                cnt[0], 1.0*cnt[0]/cnt_total,
                cnt[1], 1.0*cnt[1]/cnt_total)
            )
    return ret


    