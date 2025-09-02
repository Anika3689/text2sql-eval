import pandas as pd
def remove_setting_lines(input_file, output_file):
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            if not line.startswith("Setting"):
                outfile.write(line)

# Example usage
#remove_setting_lines("data/spider/predictions/test_dataset_predictions.txt", "data/spider/predictions/test_dataset_predictions2.txt")
print(len(pd.read_csv('data/spider/datasets_original/test_dataset.csv')))