import pandas as pd
import sqlglot
import openpyxl

if __name__ == '__main__':

    df = pd.read_csv(
        'data/spider/datasets_original/train_dataset.csv',
        quotechar='"',
        doublequote=True
    )
    rows = []

    for i in range(len(df)):
        sample = df.iloc[i]
        db_id = sample['db_id']

        if db_id != 'farm':
            continue

        nl_question = sample['question']
        sql = sample['query']

        # Replace MySQL double quotes for string literals with single quotes
        sql = sql.replace('"', "'")

        # Convert to PostgreSQL format with quoted identifiers
        converted_sql = sqlglot.transpile(
            sql,
            read="mysql",
            write="postgres",
            pretty=False,
            identify=True  # quote all identifiers
        )[0]

        rows.append({
            'question': nl_question,
            'golden_sql': converted_sql.lower()+';',
        })

    output_df = pd.DataFrame(rows)
    output_df = output_df.astype(str)
    output_df.to_excel('farm_groundtruth.xlsx', index=False, engine='openpyxl')

    print(f"Saved {len(output_df)} samples to farm_groundtruth.xlsx")
