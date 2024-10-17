import pandas as pd

df = pd.read_json('games.json').transpose()
print(df.head())