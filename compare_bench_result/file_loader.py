import pandas as pd
import pyparsing as pp
import numpy as np


class FileLoader:
    @classmethod
    def file2dataframe(cls, file_path):
        with open(file_path) as f:
            tmp_lines = f.readlines()
        column = pp.commaSeparatedList.parseString(tmp_lines[0]).asList()
        lines = []
        #header = 'timestep,label,group,name,weight,mean,cost,standard_error\n'
        header = tmp_lines[0]

        for tl in tmp_lines[1:]:
            if tl != header:
                tmp = pp.commaSeparatedList.parseString(tl).asList()
                tmp[3] = tmp[3].strip("\"")
                tmp[4] = tmp[4].strip("\"")
                lines.append(tmp)
        df = pd.DataFrame(lines, columns=column)
        df['timestep'] = df['timestep'].astype('int')
        df['mean'] = df['mean'].astype('float64')
        df['cost'] = df.cost.apply(lambda  x: float(x) if x else np.nan)
        if 'standard_error' in column:
            df['standard_error'] = df['standard_error'].astype('float64')
        if 'middle_mean' in column:
            df['middle_mean'] = df.middle_mean.apply(lambda  x: float(x) if x else np.nan)
        return df

    @classmethod
    def file_2_statement_dfs(cls, df):
        statements = []
        for g, n in df[['group', 'name']].values:
            statements.append(g + n)
        statements = set(statements)
        stmnt_dfs = []
        for s in statements:
            stmnt_dfs.append(
                df[df['group'] + df['name'] == s])
        return stmnt_dfs
