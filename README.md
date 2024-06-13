![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apache-maven&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)
![Hazelcast](https://img.shields.io/badge/Hazelcast-FF6138?style=for-the-badge&logo=hazelcast&logoColor=white)
- [Multas de estacionamiento](#multas-de-estacionamiento)
   - [Requisitos](#requisitos)
   - [Instalacion](#instalacion)
   - [Ejecucion](#ejecucion)
      - [Servidor](#servidor)
      - [Cliente](#cliente)
      - [Consultas](#consultas)
         - [Query 1](#query-1)
         - [Query 2](#query-2)
         - [Query 3](#query-3)
         - [Query 4](#query-4)
         - [Query 5](#query-5)
- [Aclaraciones sobre el proyecto](#aclaraciones-sobre-el-proyecto)

# Multas de estacionamiento
En este proyecto, se implementó un sistema para el análisis de multas de estacionamiento para los datos de las ciudades de Chicago y de Nueva York.
Para el desarrollo del mismo, se utilizó el lenguaje de programación Java y la herramienta de gestión de dependencias Maven.

Por otro lado, se utilizó la base de datos Hazelcast para almacenar los datos de manera distribuida. También, se utilizó la técnica MapReduce para el procesamiento de los datos.

## Requisitos
- Java 21.
- Maven.

## Instalacion
1. Clonar el repositorio.
2. Ubicarse dentro de la carpeta del proyecto.
3. Compilar el proyecto con el comando:

```Bash
mvn clean install
```

Luego de la compilacion del proyecto, se generaran los siguientes archivos:
- `tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz`: Contiene los clientes del sistema.
- `tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz`: Contiene el servidor del sistema.

## Ejecucion

### Servidor
Para la ejecucion del servidor, debemos seguir los siguientes pasos:
> [!NOTE]
> Para poder seguir los siguientes pasos, debemos ubicarnos dentro de la carpeta del servidor.

1. Ir al directorio del target.

    ```Bash
    cd server/target
    ```
2. Descomprimir el archivo `tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz`.

    ```Bash
   tar -xvf tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz
   ```
3. Ingresar a la carpeta descomprimida.

    ```Bash
    cd tpe2-g6-server-1.0-SNAPSHOT
   ```
4. Darle permisos de ejecucion al archivo.

    ```Bash
   chmod u+x run-server.sh
   ```
5. Ejecutar el servidor. <br>Opcionalmente, se puede indicar la máscara de red en la que se ejecutará el cluster de Hazelcast, con el parámetro `-Dmask` (por defecto, la máscara es `192.168.2.*`).

    ```Bash
   sh run-server.sh [-Dmask=<Máscara de red>] 
   ```

### Cliente
Para la ejecución del cliente, debemos seguir los siguientes pasos:
> [!NOTE]
> Para poder seguir los siguientes pasos, debemos ubicarnos dentro de la carpeta del cliente.

1. Ir al directorio del target.

    ```Bash
   cd client/target
   ```

2. Descomprimir el archivo `tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz`.

    ```Bash
   tar -xvf tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz
   ```
3. Ingresar a la carpeta descomprimida.

    ```Bash
   cd tpe2-g6-client-1.0-SNAPSHOT
   ```
4. Darle permisos de ejecucion al archivo.

    ```Bash
   chmod u+x query*.sh
   ```

### Consultas
Al ejecutar cada cliente, dentro de la carpeta que se indica como `DoutPath`, se encontrará un archivo CSV, con el resultado de la consulta, y un archivo `time.txt` indicando el tiempo que tardó el sistema, tanto en subir los datos como en ejecutar el MapReduce.

También, es muy importante que la carpeta indicada como `DinPath` tenga los archivos `ticketsCHI.csv` o `ticketsNYC.csv`, según la ciudad que se quiera analizar, al igual que los archivos `infractionsCHI.csv` o `infractionsNYC.csv`, respectivamente.
#### Query 1
La Query 1 consiste en obtener el total de multas por cada infracción.
Para ejecutar la consulta, debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query1.sh`.

    ```Bash
   sh query1.sh -Daddresses=<Dirección IP de al menos un nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde está el CSV> -DoutPath=<Carpeta donde se dejará los archivos de salida>
   ```
   Al ejecutar dicha query se genera un archivo CSV con el resultado de la consulta en la carpeta indicada como `DoutPath` con el nombre de `query1.csv`. El formato del archivo sera el siguiente:

   ```CSV
      Infraction;Tickets
   ```

#### Query 2
La Query 2 consiste en obtener las top 3 infracciones en cada barrio.
Para ejecutar la consulta, debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query2.sh`.

    ```Bash
   sh query2.sh -Daddresses=<Dirección IP de al menos un nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde está el CSV> -DoutPath=<Carpeta donde se dejará los archivos de salida>
   ```
   Al ejecutar dicha query se genera un archivo CSV con el resultado de la consulta en la carpeta indicada como `DoutPath` con el nombre de `query2.csv`. El formato del archivo sera el siguiente:
   ```CSV
   County;InfractionTop1;InfractionTop2;InfractionTop3
   ```

#### Query 3
La Query 3 consiste en obtener las top N agencias con mayor porcentaje de recaudación.
Para ejecutar la consulta, debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query3.sh`.

    ```Bash
   sh query3.sh -Daddresses=<Dirección IP de al menos un nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde está el CSV> -DoutPath=<Carpeta donde se dejará los archivos de salida> -Dn=<Número de agencias>
   ```
   Al ejecutar dicha query se genera un archivo CSV con el resultado de la consulta en la carpeta indicada como `DoutPath` con el nombre de `query3.csv`. El formato del archivo sera el siguiente:
   ```CSV
   Issuing Agency;Percentage
   ```

#### Query 4
La Query 4 consiste en obtener la patente con más infracciones de cada barrio en el rango [from, to].
Para ejecutar la consulta, debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query4.sh`.

    ```Bash
   sh query4.sh -Daddresses=<Dirección IP de al menos un nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde está el CSV> -DoutPath=<Carpeta donde se dejará los archivos de salida> -Dfrom=<Fecha desde> -Dto=<Fecha hasta>
   ```
   Al ejecutar dicha query se genera un archivo CSV con el resultado de la consulta en la carpeta indicada como `DoutPath` con el nombre de `query4.csv`. El formato del archivo sera el siguiente:
   ```CSV
   County;Plate;Tickets
   ```

> [!NOTE]
> Los parametros `-Dfrom` y `-Dto` deben ir en el siguiente formato: `dd/MM/yyyy`.

#### Query 5
La Query 5 consiste en obtener pares de infracciones que tienen, en grupos de a cientos, el mismo promedio de monto de multa.
Para ejecutar la consulta, debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query5.sh`.

    ```Bash
   sh query5.sh -Daddresses=<Dirección IP de al menos un nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde está el CSV> -DoutPath=<Carpeta donde se dejará los archivos de salida>
   ```
   Al ejecutar dicha query se genera un archivo CSV con el resultado de la consulta en la carpeta indicada como `DoutPath` con el nombre de `query5.csv`. El formato del archivo sera el siguiente:
   ```CSV
   Group;Infraction A;Infraction B
   ```

# Aclaraciones sobre el proyecto
Este proyecto es realizado para la materia Programación de Objetos Distribuidos del ITBA.
**Los integrantes del grupo son:**
- 62248 - [José Rodolfo Mentasti](https://github.com/JoseMenta)
- 62618 - [Axel Facundo Preiti Tasat](https://github.com/AxelPreitiT)
- 62500 - [Gastón Ariel Francois](https://github.com/francoisgaston)
- 62329 - [Lautaro Hernando](https://github.com/laucha12)
