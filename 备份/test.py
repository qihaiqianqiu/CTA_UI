from sklearn.datasets import load_iris
from sklearn.tree import DecisionTreeClassifier, plot_tree
import matplotlib.pyplot as plt

# 加载数据集
iris = load_iris()

# 创建决策树模型
clf = DecisionTreeClassifier()

# 拟合模型
clf.fit(iris.data, iris.target)

# 可视化决策树
plt.figure(figsize=(20,10))
plot_tree(clf, feature_names=iris.feature_names, class_names=iris.target_names, filled=True)
plt.show()