import questdb.ingress as qi
import pandas as pd

def main():
    df = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [1.24423, 2.234, 3.234],
        'c': ['A', 'B', 'C'],
        'ts': [
            pd.Timestamp('2020-01-01'),
            pd.Timestamp('2020-01-02'),
            pd.Timestamp('2020-01-03')]})
    
    buf = qi.Buffer()
    buf.dataframe(df, table_name='test', at='ts')
    print(buf)
