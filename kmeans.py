# Iris kmeans clustering
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn import datasets

# Load the iris dataset
iris = datasets.load_iris()
X = iris.data
y = iris.target

# Create a DataFrame with the iris data
df = pd.DataFrame(X, columns=iris.feature_names)

# Create a kmeans model on our data, using 3 clusters
kmeans = KMeans(n_clusters=3)
kmeans.fit(df)

# Get the cluster centroids
centroids = kmeans.cluster_centers_

print(centroids)