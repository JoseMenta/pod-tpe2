import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

'''
Nodos,Combiner,Cantidad,Key,Load_data,Map_reduce,Start loading data,Finished loading data,Start MapReduce,End MapReduce,Query,File
'''

def query_load_time_full(df):
    # Filtros
    df = df[df['Nodos'] == 4]
    df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 15000000]
    df = df[df['Key'] == "Date"]
    #df = df[df['Query'] == "Q1"]
    df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    queries = df['Query'].unique()
    indices = np.arange(len(queries))
    files = df['File'].unique()
    num_files = len(files)
    bar_width = 0.8 / num_files

    plt.figure(figsize=(10, 6))

    for i, file in enumerate(files):
        file_data = df[df['File'] == file]
        positions = indices + i * bar_width
        plt.bar(positions, file_data['Load_data'], width=bar_width, label=file)

    plt.ylabel('Tiempo en carga (s)', fontsize=16)
    plt.title('Tiempo en carga de 15 millos de registros para cada Query', fontsize=16)
    plt.xticks(indices + bar_width * (num_files - 1) / 2, queries)
    plt.grid(False)
    plt.show()

def query_map_time_full(df):
    # Filtros
    df = df[df['Nodos'] == 4]
    df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 15000000]
    df = df[df['Key'] == "Date"]
    #df = df[df['Query'] == "Q1"]
    df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    queries = df['Query'].unique()
    indices = np.arange(len(queries))
    files = df['File'].unique()
    num_files = len(files)
    bar_width = 0.8 / num_files

    plt.figure(figsize=(10, 6))

    for i, file in enumerate(files):
        file_data = df[df['File'] == file]
        positions = indices + i * bar_width
        plt.bar(positions, file_data['Map_reduce'], width=bar_width, label=file)

    plt.ylabel('Tiempo en map/reduce (s)', fontsize=16)
    plt.title('Tiempo en map/reduce de 15 millos de registros para cada Query', fontsize=16)
    plt.xticks(indices + bar_width * (num_files - 1) / 2, queries)
    plt.grid(False)
    plt.show()

def query_load_time_1N(df):
    # Filtros
    df = df[df['Nodos'] == 1]
    df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 5000000]
    df = df[df['Key'] == "Date"]
    #df = df[df['Query'] == "Q1"]
    #df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Lauta"]

    queries = df['Query'].unique()
    indices = np.arange(len(queries))
    files = df['File'].unique()
    num_files = len(files)
    bar_width = 0.8 / num_files

    plt.figure(figsize=(10, 6))

    for i, file in enumerate(files):
        file_data = df[df['File'] == file]
        positions = indices + i * bar_width
        plt.bar(positions, file_data['Load_data'], width=bar_width, label=file)

    plt.ylabel('Tiempo en carga (s)', fontsize=16)
    plt.title('Tiempo en carga de 5 millos de registros para cada Query', fontsize=16)
    plt.xticks(indices + bar_width * (num_files - 1) / 2, queries)
    plt.legend(title='File')
    plt.grid(False)
    plt.show()

def query_map_time_1N(df):
    # Filtros
    df = df[df['Nodos'] == 1]
    df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 5000000]
    df = df[df['Key'] == "Date"]
    #df = df[df['Query'] == "Q1"]
    #df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Lauta"]

    queries = df['Query'].unique()
    indices = np.arange(len(queries))
    files = df['File'].unique()
    num_files = len(files)
    bar_width = 0.8 / num_files

    plt.figure(figsize=(10, 6))

    for i, file in enumerate(files):
        file_data = df[df['File'] == file]
        positions = indices + i * bar_width
        plt.bar(positions, file_data['Map_reduce'], width=bar_width, label=file)

    plt.ylabel('Tiempo en Map/Reduce', fontsize=16)
    plt.title('Tiempo en Map/Reduce de 5.000.000 para cada Query', fontsize=16)
    plt.xticks(indices + bar_width * (num_files - 1) / 2, queries)
    plt.legend(title='File')
    plt.grid(False)
    plt.show()

def combiner_load_analysis(df):
    # Filtros
    #df = df[df['Nodos'] == 1]
    #df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 5000000]
    df = df[df['Key'] == "Date"]
    df = df[df['Query'] == "Q1"]
    #df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    plt.figure(figsize=(10, 6))

    for combiner, group in df.groupby('Combiner'):
        group = group.sort_values(by='Nodos')
        if combiner:
            label = 'Activado'
        else:
            label = 'Desactivado'
        plt.plot(group['Nodos'], group['Load_data'], marker='o', label=f'{label}')

    plt.xlabel('Cantidad de nodos')
    plt.ylabel('Tiempo en carga de datos (s)', fontsize=16)
    plt.title('Tiempo en carga de datos vs Cantidad de nodos', fontsize=16)
    plt.xticks(np.arange(1, 4+1, step=1))
    plt.legend(title='Combiner')
    plt.grid(False)
    plt.show()

def combiner_map_analysis(df):
    # Filtros
    #df = df[df['Nodos'] == 1]
    #df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 5000000]
    df = df[df['Key'] == "Date"]
    df = df[df['Query'] == "Q1"]
    #df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    plt.figure(figsize=(10, 6))

    for combiner, group in df.groupby('Combiner'):
        group = group.sort_values(by='Nodos')
        if combiner:
            label = 'Activado'
        else:
            label = 'Desactivado'
        plt.plot(group['Nodos'], group['Map_reduce'], marker='o', label=f'{label}')

    plt.xlabel('Cantidad de nodos')
    plt.ylabel('Tiempo en map/reduce (s)', fontsize=16)
    plt.title('Tiempo en map/reduce vs Cantidad de nodos', fontsize=16)
    plt.xticks(np.arange(1, 4+1, step=1))
    plt.legend(title='Combiner')
    plt.grid(False)
    plt.show()

def cantidad_map_analysis(df):
    # Filtros
    #df = df[df['Nodos'] == 1]
    df = df[df['Combiner'] == True]
    #df = df[df['Cantidad'] == 5000000]
    df = df[df['Key'] == "Date"]
    df = df[df['Query'] == "Q1"]
    df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    plt.figure(figsize=(10, 6))

    for combiner, group in df.groupby('Nodos'):
        group = group.sort_values(by='Nodos')
        plt.plot(group['Cantidad'], group['Map_reduce'], marker='o', label=f'N = {combiner}')

    plt.xlabel('Cantidad de infracciones')
    plt.ylabel('Tiempo en map/reduce (s)', fontsize=16)
    plt.title('Tiempo en map/reduce vs Cantidad de infracciones', fontsize=16)
    plt.legend(title='Nodos')
    plt.grid(False)
    plt.show()

def cantidad_load_analysis(df):
    # Filtros
    #df = df[df['Nodos'] == 1]
    df = df[df['Combiner'] == True]
    #df = df[df['Cantidad'] == 5000000]
    df = df[df['Key'] == "Date"]
    df = df[df['Query'] == "Q1"]
    df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    plt.figure(figsize=(10, 6))

    for combiner, group in df.groupby('Nodos'):
        plt.plot(group['Cantidad'], group['Load_data'], marker='o', label=f'N = {combiner}')

    plt.xlabel('Cantidad de infracciones')
    plt.ylabel('Tiempo en carga de datos (s)', fontsize=16)
    plt.title('Tiempo en carga de datos vs Cantidad de infracciones', fontsize=16)
    plt.legend(title='Nodos')
    plt.grid(False)
    plt.show()

def key_map_analysis(df):
    # Filtros
    #df = df[df['Nodos'] == 1]
    df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 5000000]
    #df = df[df['Key'] == "Date"]
    df = df[df['Query'] == "Q1"]
    df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    plt.figure(figsize=(10, 6))

    for combiner, group in df.groupby('Key'):
        plt.plot(group['Nodos'], group['Map_reduce'], marker='o', label=f'{combiner}')

    plt.xlabel('Cantidad de nodos')
    plt.ylabel('Tiempo en Map/reduce (s)', fontsize=16)
    plt.title('Tiempo en Map/reduce vs Cantidad de nodos', fontsize=16)
    plt.xticks(np.arange(1, 4+1, step=1))
    plt.legend(title='Key')
    plt.grid(False)
    plt.show()

def key_load_analysis(df):
    # Filtros
    #df = df[df['Nodos'] == 1]
    df = df[df['Combiner'] == True]
    df = df[df['Cantidad'] == 5000000]
    #df = df[df['Key'] == "Date"]
    df = df[df['Query'] == "Q1"]
    df = df[df['File'] == "NYC"]
    df = df[df['Dev'] == "Jose"]

    plt.figure(figsize=(10, 6))

    for combiner, group in df.groupby('Key'):
        plt.plot(group['Nodos'], group['Load_data'], marker='o', label=f'{combiner}')

    plt.xlabel('Cantidad de nodos')
    plt.ylabel('Tiempo en carga de datos (s)', fontsize=16)
    plt.title('Tiempo en carga de datos vs Cantidad de nodos', fontsize=16)
    plt.xticks(np.arange(1, 4+1, step=1))
    plt.legend(title='Key')
    plt.grid(False)
    plt.show()

def filter_time_map(df):
    # Filtros
    df = df[df['Dev'] == "Extra"]

    files = df['Key'].unique()
    print(files)

    plt.figure(figsize=(10, 6))

    for file in files:
        subset = df[df['Key'] == file]
        plt.bar(file, subset['Map_reduce'].values[0], label=file)

    plt.ylabel('Tiempo en map/reduce (s)', fontsize=16)
    plt.title('Tiempo en map/reduce de 15 millos de registros para Query 4', fontsize=16)
    plt.grid(False)
    plt.show()

def filter_time_load(df):
    # Filtros
    df = df[df['Dev'] == "Extra"]

    files = df['Key'].unique()
    print(files)

    plt.figure(figsize=(10, 6))

    for file in files:
        subset = df[df['Key'] == file]
        plt.bar(file, subset['Load_data'].values[0], label=file)

    plt.ylabel('Tiempo en carga (s)', fontsize=16)
    plt.title('Tiempo en carga de 15 millos de registros para Query 4', fontsize=16)
    plt.grid(False)
    plt.show()

def main():
    df = pd.read_csv('Data.csv')
    filter_time_load(df)
    filter_time_map(df)
    query_load_time_full(df)
    query_map_time_full(df)
    query_map_time_1N(df)
    query_load_time_1N(df)
    combiner_load_analysis(df)
    combiner_map_analysis(df)
    cantidad_load_analysis(df)
    cantidad_map_analysis(df)
    key_map_analysis(df)
    key_load_analysis(df)
    

if __name__ == "__main__":
    main()