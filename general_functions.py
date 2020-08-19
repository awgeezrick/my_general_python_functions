import string
import os
import pandas as pd
import xml.etree.ElementTree as et
import random
from datetime import datetime as dt
from datetime import timedelta


def count_files(recog_dir):
    import fnmatch
    return len(fnmatch.filter(os.listdir(recog_dir), '*.xlsx'))


def file_to_dataframe(file, filetype):
    if filetype == 'csv':
        df = pd.read_csv(file)
        return df
    if filetype == "excel":
        df = pd.read_excel(file)
        return df


def search_eq(df, col, term):
    result = df[(df[col] == term)]
    return result


def search_con(df, col, term):
    result = df[(df[col].str.contains(term))]
    return result


def show_nulls(df, column_name):
    df_result = df[pd.isnull(df[column_name])]
    return df_result


def show_not_nulls(df, column_name):
    df_result = df[pd.notnull(df[column_name])]
    return df_result


def convert_floats_to_ints(df):
    df[df.select_dtypes(['float64']).columns] = df.select_dtypes(['float64']).apply(lambda x: x.astype(int))
    return df


def find_dupes(df, colname):
    df = df[df.duplicated(colname, keep=False)]
    return df


def random_string(length):
    letters = []
    for l in range(length):
        letters.append(random.choice(string.ascii_letters))
    return ''.join(letters)


def del_columns(df):
    del df['field_name']
    del df['field_data']
    return df


def remove_dupes(sourcedf, colname):
    df = sourcedf
    df = df.drop_duplicates(subset=colname, keep='first', inplace=False)
    return df


def delete_columns(df, cols):
    df.drop(cols, axis=1, inplace=True)
    return print("columns deleted")


def df_to_csv(df, filename, csvpath):
    df.to_csv(csvpath + filename + '.csv', sep=',', index=False)
    return print("CSV file created.")


def df_to_excel(df, filename, csvpath):
    writer = pd.ExcelWriter(csvpath + filename + '.xlsx')
    df.to_excel(writer, index=False)
    writer.save()
    return print("Excel file created.")


def is_in_criteria(df1, df2, col):
    criteria = df1[col].isin(df2[col])
    df = df1[criteria]
    return df


def is_not_in_criteria(df1, df2, col):
    criteria = ~df1[col].isin(df2[col])
    df = df1[criteria]
    return df


def month_string_to_number(str_item):
    m = {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr': 4,
        'may': 5,
        'jun': 6,
        'jul': 7,
        'aug': 8,
        'sep': 9,
        'oct': 10,
        'nov': 11,
        'dec': 12
    }

    if str_item.isnumeric():
        return str_item
    else:
        s = str_item.strip()[:3].lower()
        out = m[s]
        return out


def municipality_to_city(str_item):
    m = ['North York', 'Scarborough', 'Etobicoke', 'York', 'East York', 'Toronto']

    if str_item in m:
        return 'Toronto'
    else:
        return str_item


def query_to_sql(fname, query, sql_path):
    dest = sql_path + '\\sql_files\\'
    ext = '.sql'
    filename = dest + fname + ext
    f = open(filename, "w")
    f.write(query)
    f.close()
    return print('query written to sql file')


def convert_html(dfhtml):
    import html2text as h2t
    conv_text = h2t.html2text(dfhtml)
    return conv_text


def col_to_list(df, col_name):
    df_lst = df[col_name]
    # convert dataframe to list in order to use in the sql queries
    just_lst = df_lst.tolist()
    return just_lst


def delete_files(folder_path, files_to_delete):
    import os
    # folder_files = os.listdir(folder_path)
    for item in files_to_delete:
        if os.path.exists(os.path.join(folder_path, item)):
            os.remove(os.path.join(folder_path, item))
            print('the file:' + item + ' has been removed.')

    return


def change_go_format(df, col):
    df[col] = df[col].str.replace('GO-', '')
    df[col] = df[col].str[:4] + "-" + df[col].str[4:]
    return df


def convert_go_format(go):
    vg = go.replace('GO-', '')
    vg_f = vg[:4] + "-" + vg[4:]
    return vg_f


def revert_go_format(go):
    vg = go.replace('-', '')
    vg_f = 'GO-' + vg
    return vg_f


def parse_xml(xml_file, df_cols):
    """Parse the input XML file and store the result in a pandas
    DataFrame with the given columns.

    The first element of df_cols is supposed to be the identifier
    variable, which is an attribute of each node element in the
    XML data; other features will be parsed from the text content
    of each sub-element.
    """

    xtree = et.parse(xml_file)
    xroot = xtree.getroot()
    rows = []

    for node in xroot:
        res = [node.attrib.get(df_cols[0])]
        for el in df_cols[1:]:
            if node is not None and node.find(el) is not None:
                res.append(node.find(el).text)
            else:
                res.append(None)
        rows.append({df_cols[i]: res[i]
                     for i, _ in enumerate(df_cols)})

    out_df = pd.DataFrame(rows, columns=df_cols)
    return out_df


def remove_whitespace(x):
    return "".join(x.split())


def strip_digits(s):
    import re
    return re.sub("\\d+", "", s)


def is_in_criteria_mult_col(df1, df2, col1, col2):
    criteria = df1[col1].isin(df2[col2])
    df = df1[criteria]
    return df


def is_not_in_criteria_mult_col(df1, df2, col1, col2):
    criteria = ~df1[col1].isin(df2[col2])
    df = df1[criteria]
    return df


def tracefunc(frame, event, indent):
    if event == "call":
        indent[0] += 2
        print("-" * indent[0] + "> call function", frame.f_code.co_name)
    elif event == "return":
        print("<" + "-" * indent[0], "exit function", frame.f_code.co_name)
        indent[0] -= 2
    return tracefunc


def generate_unique_id():
    import uuid
    unique_id = uuid.uuid4()
    return unique_id


def percentage(part, whole):
    result = 100 * float(part) / float(whole)
    # return "{:.1%}".format(result)
    final = '%' + "{0:.2f}".format(result)
    return final


def empty_folder(folder_path):
    import os
    folder_files = os.listdir(folder_path)
    for item in folder_files:
        os.remove(os.path.join(folder_path, item))
    return print('folder empty')


def convert_int64_to_ints(df):
    df[df.select_dtypes(['int64']).columns] = df.select_dtypes(['int64']).apply(lambda x: x.astype(int))
    return df


def diff(li1, li2):
    return list(set(li1) - set(li2))


def calculate_age(dob):
    today = dt.today()
    born = pd.to_datetime(dob, format='%Y-%m-%d', errors='coerce')
    try:
        birthday = born.replace(year=today.year)

        # raised when birth date is February 29
    # and the current year is not a leap year
    except ValueError:
        birthday = born.replace(year=today.year,
                                month=born.month + 1, day=1)

    if birthday > today:
        return today.year - born.year - 1
    else:
        return today.year - born.year


def get_null_counts(df):
    nullcounts = df.isnull().sum(axis=0)
    tmp = pd.DataFrame(data=nullcounts, columns=['total_nulls'])
    tmp['perc_of_total'] = tmp.apply(lambda x: (x['total_nulls'] / 1629) * 100, axis=1)
    tmp.sort_values(by=['total_nulls'], ascending=False, inplace=True)
    tmp.index.set_names(['fields'], inplace=True)
    tmp.reset_index(inplace=True)
    return tmp


def missing_values_df(df):
    n_records = len(df)
    for column in df:
        print("{} | {} | {}".format(
            column, len(df[df[column].isnull()]) / (1.0 * n_records), df[column].dtype
        ))
    return


def cardinality_categorical(df):
    n_records = len(df)
    for column in df.select_dtypes([object]):
        print("{} | uniques/records: {:.3f} | Minimum observations: {:.3f}".format(
            column,
            len(df[column].unique()) / n_records,
            df[column].value_counts().min()
        ))
    return


def outliers_col(df):
    from scipy import stats
    import numpy as np
    for column in df:
        if df[column].dtype != np.object:
            n_outliers = len(df[(np.abs(stats.zscore(df[column])) > 3) & (df[column].notnull())])
            print("{} | {} | {}".format(
                df[column].name,
                n_outliers,
                df[column].dtype
            ))
    return


def quick_counts(dfc):
    result = pd.value_counts(dfc)[:]
    return result


def drop_dupes_reset_idx(df):
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    return print('dupes dropped and index reset.')


def drop_col_from_df(df, cols):
    df.drop(columns=[cols], inplace=True)
    return print(cols + ' has been dropped from dataframe')


def find_file_in_directory(dir_path, keyword):
    data_folder = os.listdir(dir_path)
    for item in data_folder:
        if keyword in item:
            file_path = os.path.join(dir_path, item)
            return file_path


def df_to_html(df, fn):
    html_file = fn + '.html'
    df.to_html(html_file)
    return print(html_file + ' has been created')


def empty_html_file_folder(html_files_folder_path):
    htmlfiles = os.listdir(html_files_folder_path)
    for item in htmlfiles:
        if item.endswith(".html"):
            os.remove(os.path.join(html_files_folder_path, item))
    return print('html files folder has been emptied.')


def prev_df_to_html_table(df, filename, title, html_files_folder_path):
    pd.set_option('colheader_justify', 'center')  # FOR TABLE <th>
    html_string = '''
    <html>
      <head>
      <title>{title}</title>
      {styling}
      </head>

      <body>
      <h1>{title}</h1>
        {table}
      </body>
    </html>
    '''
    css_style = """
    <style>
    h1 {text-align:center;}

    .mystyle {
        font-size: 11pt;
        font-family: Arial;
        border-collapse: collapse;
        border: 1px solid silver;

    }

    .mystyle td, th {
        padding: 15px;
    }
/*
    .mystyle tr:nth-child(even) {
        background: #E0E0E0;
    }
*/
    </style>
    """
    df_html_filename = html_files_folder_path + filename + '.html'

    # OUTPUT AN HTML FILE
    with open(df_html_filename, 'w') as f:
        f.write(html_string.format(title=title, styling=css_style, table=df.to_html(classes='mystyle', index=False)))
    return print('html file created')


def table_styles():
    return [dict(selector="td",
                 props=[('border', '1px solid #f0f0f0'),
                        ('padding', '15px')
                        ]),
            dict(selector="th",
                 props=[('border', '1px solid #f0f0f0'),
                        ('padding', '15px')
                        ])]


def for_excel_highlight_odd(s):
    if s.eo == 'odd':
        return ['background-color: #f0f0f0'] * 12
    else:
        return ['background-color: white'] * 12


# this one works WAAAAAAAY better since I don't need to hard code the number of columns
def highlight_odd(s):
    """
    highlight if odd
    """
    if s == 'odd':
        return 'background-color: #f0f0f0'
    else:
        return ''


def df_size(df):
    return df.shape[0]


def get_updated_date_stuff(df, date_col_orig):
    date_col = 'date_temp'
    df[date_col] = pd.to_datetime(df[date_col_orig], format='%Y-%m-%d', errors='coerce')
    df['day_of_week'] = df[date_col].dt.day_name()
    df['month'] = df[date_col].dt.month_name()
    df['year'] = df[date_col].dt.year
    df['year'].fillna(0, inplace=True)
    df['year'] = df['year'].astype(int)
    df.drop(columns=[date_col], inplace=True)
    return df


# create function accepting a single parameter, the year as a four digit number
def get_random_date(year):
    # try to get a date
    try:
        return dt.strptime('{} {}'.format(random.randint(1, 366), year), '%Y-%m-%d')

    # if the value happens to be in the leap year range, try again
    except ValueError:
        get_random_date(year)


def get_random_number_of_rows(df, number_of_records):
    rndm = df.sample(n=number_of_records)
    rndm.reset_index(drop=True, inplace=True)
    return rndm


def get_single_random_value_from_dataframe(df, col):
    return df[col].sample(n=1).iloc[0]


def get_random_percentage_of_rows(df, percentage_of_records):
    rndm = df.sample(frac=percentage_of_records)
    rndm.reset_index(drop=True, inplace=True)
    return rndm


def get_random_percentage_of_rows_alt(df, percentage_of_records):
    rndm = df.sample(frac=percentage_of_records, replace=True)
    rndm.reset_index(drop=True, inplace=True)
    return rndm


def get_random_date_within_31days():
    today = dt.today()
    return dt.date(today - timedelta(days=random.randint(1, 31))).strftime('%Y-%m-%d')


def get_random_date_for_dob():
    today = dt.today()
    return dt.date(today - timedelta(days=random.randint(5840, 25550))).strftime('%Y-%m-%d')


def letters_to_aplphabet_position(text):
    from itertools import count
    from string import ascii_lowercase
    letter_mapping = dict(zip(ascii_lowercase, count(1)))
    indexes = [
        letter_mapping[letter] for letter in text.lower()
        if letter in letter_mapping
    ]

    return ''.join(str(index) for index in indexes)


def create_concat_of_df_copies(df, dest_num):
    x = 0
    num = int(dest_num / df.shape[0])
    num_test = int(df.shape[0]) * num
    if num_test < dest_num:
        num_final = num + 1
    else:
        num_final = num
    df2 = []
    while num_final > x:
        df2.append(df)
        x += 1
    df_final = pd.concat(df2)
    df_final.reset_index(drop=True, inplace=True)
    return df_final


def get_dataframe_memory_usage(df):
    return df.info(memory_usage='deep')


def optimize_df(df):
    df[df.select_dtypes(['int64']).columns] = df.select_dtypes(['int64']).apply(lambda x: x.astype('int32'))
    df[df.select_dtypes(['float64']).columns] = df.select_dtypes(['float64']).apply(lambda x: x.astype('float32'))
    df[df.select_dtypes(['object']).columns] = df.select_dtypes(['object']).apply(lambda x: x.astype('category'))
    return
