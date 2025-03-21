import pandas as pd

# Cleaning the target csv file
def target(df):
    df['Target'] = df['Target'].str.replace('[$,]', '', regex=True).astype(int)
    df['TargetMonth'] = pd.to_datetime(df['TargetMonth'], format='%A, %B %d, %Y')
    df['Month'] = df['TargetMonth'].dt.strftime('%B')
    df['Year'] = df['TargetMonth'].dt.year
    df['EmployeeID'] = df['EmployeeID'].astype(int)
    df = df.drop(columns="TargetMonth")
    return df

# Cleaning the Sales csv file
def sales(df):
    df['Unit Price'] = df['Unit Price'].str.replace(',', '').str.replace('$','').astype(float)
    df['Sales'] = df['Sales'].str.replace(',', '').str.replace('$','').astype(float)
    df['Cost'] = df['Cost'].str.replace(',', '').str.replace('$','').astype(float)
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    return df


# Cleaning the Products csv file
def products(df):
    df['Standard Cost'] = df['Standard Cost'].str.replace('[$,]', '', regex=True).astype(float)
    df = df.drop(columns="Background Color Format")
    df = df.drop(columns="Font Color Format")
    return df