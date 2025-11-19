from read import read
from transform import transform
from load import load

if __name__ == "__main__":
    read_data = read("healthcare_dataset.csv")
    transformed_data = transform(read_data)
    loaded_data = load(transformed_data)
