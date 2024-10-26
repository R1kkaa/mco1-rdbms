Requirements
- Use 2 databases, 1 for the actual database, 1 for the functional tests with sampled data

Modules Required:
- Prisma

Python Libraries Required:
- pandas
- numpy
- SQLAlchemy
- python-dotenv
- mysql-connector-python
- matplotlib
- seaborn

.env file contents:
DATABASE_URL="mysql://(user):(pass)@localhost:(portnumber)/(database-name)"
ALCHEMY_DATABASE_URL="mysql+mysqlconnector://(user):(pass)@localhost:(portnumber)/(database-name)"
ALCHEMY_DATABASE_URL-SAMPLE="mysql+mysqlconnector://(user):(pass)@localhost:(portnumber)/(sample-database-name)"
