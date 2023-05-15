import networkx as nx
import matplotlib.pyplot as plt

pointList = ['A2305', 'A2306', 'A2307', 'A2309', 'A2310']
linkList = [('A2305', 'A2306'), ('A2306', 'A2307'), ('A2309', 'A2310')]


def subgraph():
    G = nx.DiGraph()
    # 转化为图结构
    for node in pointList:
        G.add_node(node)

    for link in linkList:
        G.add_edge(link[0], link[1])
        G.add_edge(link[1], link[0])

    # 画图
    plt.subplot(331)
    nx.draw_networkx(G, with_labels=True)
    color =['y','g','r','b','w']
    subplot_pos = [333,334,335,336,337,338,339]
    # 打印连通子图
    for c in nx.strongly_connected_components(G):
       # 得到不连通的子集
        nodeSet = G.subgraph(c).nodes()
       # 绘制子图
        subgraph = G.subgraph(c)
        print(subplot_pos[0])
        plt.subplot(subplot_pos[0])  # 第二整行
        nx.draw_networkx(subgraph, with_labels=True,node_color=color[0])
        color.pop(0)
        subplot_pos.pop(0)

    plt.show()
subgraph()