from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, Boolean, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import csv
import pandas as pd

class DataCleaningPipeline:
    
    def __init__(self, dbname='examples', user='examples', password='examples', host='localhost', port='5432'):
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.metadata = MetaData()

    def export_dirty_data_to_csv(self, source_table, csv_file):
        try:
            self.metadata.reflect(bind=self.engine, only=[source_table])
            table = self.metadata.tables[source_table]
            results = self.session.query(table).all()
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([column.name for column in table.columns])
                for row in results:
                    writer.writerow([getattr(row, column.name) for column in table.columns])
            print(f"Data exported from {source_table} to {csv_file}")
            return csv_file
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.session.close()

    def create_table_with_columns(self, table_name, columns):
        new_table = Table(table_name, self.metadata)
        # Mapping of pandas dtypes to SQLAlchemy types
        dtype_map = {
            'object': String,
            'int64': Integer,
            'float64': Float,
            'datetime64[ns]': DateTime,
            'bool': Boolean
        }

        for col_name, col_type in columns.items():
            # Default to String if dtype not recognized
            sqla_type = dtype_map.get(str(col_type), String)
            new_table.append_column(Column(col_name, sqla_type))
            
        # Creating an inspector object
        inspector = inspect(self.engine)

        # Check if table exists, then drop if it does
        if inspector.has_table(table_name):
            new_table.drop(self.engine)

        # Create the new table
        new_table.create(self.engine) 

    def import_clean_data_to_table(self, table_name, csv_file):
        try:
            table = self.metadata.tables[table_name]
            with open(csv_file, 'r') as file:
                reader = csv.DictReader(file)
                data_to_insert = [row for row in reader]

                with self.engine.connect() as conn:
                    trans = conn.begin()  # Explicitly begin a transaction
                    conn.execute(table.insert(), data_to_insert)
                    trans.commit()  # Explicitly commit the transaction

                print(f"Data imported into {table_name} from {csv_file}")
        except Exception as e:
            print(f"Error: {e}")
            trans.rollback()  # Rollback in case of error


 
