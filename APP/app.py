#libaries used
#dash -> pip install dash

#run with python app.py

from dash import Dash, html, dash_table
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv(override=True)
engine = create_engine(os.environ.get("ALCHEMY_DATABASE_URL"), connect_args={'connect_timeout': 600})

app = Dash()

#test query, displays all developers
query = "SELECT * FROM developer"
df = pd.read_sql(query, engine)

app.layout = [
    html.Div(children='TEST'),
    dash_table.DataTable(data=df.to_dict('records'))
    ]

if __name__ == '__main__':
    app.run(debug=True)