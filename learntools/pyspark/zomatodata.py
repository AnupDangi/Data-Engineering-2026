import kagglehub

# Download latest version
path = kagglehub.dataset_download("saurabhbadole/zomato-delivery-operations-analytics-dataset")

print("Path to dataset files:", path)