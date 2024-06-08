![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apache-maven&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)
![Hazelcast](https://img.shields.io/badge/Hazelcast-FF6138?style=for-the-badge&logo=hazelcast&logoColor=white)

# Multas de estacionamiento
En este proyecto se implemento un sistema para el analisis de multas de estacionamiento para los datos de la ciudad de Chicago y de Nueva York.
Para el desarrollo del mismos se utilizo el lenguaje de programacion Java y la herramienta de gestion de dependencias Maven.

Por otro lado se utilizo la base de datos Hazelcast para almacenar los datos de manera distribuida. Tambien se utilizo la tecnologia MapReduce para el procesamiento de los datos.

## Requisitos
- Java 21.
- Maven.

## Instalacion
1. Clonar el repositorio.
2. Ubicarse dentro de la carpeta del proyecto.
3. Compilar el proyecto con el comando:
```mvn clean install```

Luego de la compilacion del proyecto se generaran los siguientes archivos:
- `tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz`: Contiene el cliente del sistema.
- `tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz`: Contiene el servidor del sistema.

## Ejecucion

### Servidor
Para la ejecucion del servidor debemos seguir los siguientes pasos:
> [!NOTE]
> Para poder seguir los siguientes pasos debemos ubicarnos dentro de la carpeta del servidor.

1. Ir al directorio del targe.
```cd server/target```
2. Descomprimir el archivo `tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz`.
```tar -xvf tpe2-g6-server-1.0-SNAPSHOT-bin.tar.gz```
3. Ingresar a la carpeta descomprimida.
```cd tpe2-g6-server-1.0-SNAPSHOT```
4. Darle permisos de ejecucion al archivo
```chmod u+x run-server.sh```
5. Ejecutar el servidor.
```./run-server.sh ```

### Cliente
Para la ejecucion del cliente debemos seguir los siguientes pasos:
> [!NOTE]
> Para poder seguir los siguientes pasos debemos ubicarnos dentro de la carpeta del cliente.

1. Ir al directorio del targe.
```cd client/target```
2. Descomprimir el archivo `tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz`.
```tar -xvf tpe2-g6-client-1.0-SNAPSHOT-bin.tar.gz```
3. Ingresar a la carpeta descomprimida.
```cd tpe2-g6-client-1.0-SNAPSHOT```
4. Darle permisos de ejecucion al archivo
```chmod u+x query*.sh```

### Consultas
Al ejecutar cada cliente dentro de la carpeta, que se indica como `DoutPath`, se encontrara un CSV con el resultado de la consulta y un archivo `time.txt` con el tiempo que tardo el sistema tanto en subir los datos como en ejecutar el MapReduce.
#### Query 1
La Query 1 consiste en obtener el total de multas por cada infraccion.
Para ejecutar la consulta debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query1.sh`.
```sh query1.sh -Daddresses=<Direccion IP del un Nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde esta el CSV> -DoutPath=<Carpeta donde se dejara los archivos de salida>```

#### Query 2
La Query 2 consiste en obtener las top 3 infracciones en cada barrio.
Para ejecutar la consulta debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query2.sh`.
```sh query2.sh -Daddresses=<Direccion IP del un Nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde esta el CSV> -DoutPath=<Carpeta donde se dejara los archivos de salida>```

#### Query 3
La Query 3 consiste en obtener las top N agencias con mayor porcentaje de recaudación.
Para ejecutar la consulta debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query3.sh`.
```sh query3.sh -Daddresses=<Direccion IP del un Nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde esta el CSV> -DoutPath=<Carpeta donde se dejara los archivos de salida> -Dn=<Numero de agencias>```

#### Query 4
La Query 4 consiste en obtener la patente con más infracciones de cada barrio en el rango [from, to].
Para ejecutar la consulta debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query4.sh`.
```sh query4.sh -Daddresses=<Direccion IP del un Nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde esta el CSV> -DoutPath=<Carpeta donde se dejara los archivos de salida> -Dfrom=<Fecha desde> -Dto=<Fecha hasta>```

> [!NOTE] Parametros
> Los parametros From y To deben ir en el siguiente formato `dd/MM/yyyy`.

#### Query 5
La Query 5 consiste en obtener pares de infracciones que tienen, en grupos de a cientos, el mismo promedio de monto de multa.
Para ejecutar la consulta debemos seguir los siguientes pasos:
1. Ejecutar el archivo `query5.sh`.
```sh query5.sh -Daddresses=<Direccion IP del un Nodo de Hazelcast> -Dcity=<NYC O CHI>  -DinPath=<Caperta donde esta el CSV> -DoutPath=<Carpeta donde se dejara los archivos de salida>```

# Aclaraciones sobre el proyecto
Este mismo proyecto es realizado para la meteria Programacion de Objetos Distribuidos del ITBA.
**Los integrantes del grupo son:**
- 62248 - [José Rodolfo Mentasti](https://github.com/JoseMenta)
- 62618 - [Axel Facundo Preiti Tasat](https://github.com/AxelPreitiT)
- 62500 - [Gastón Ariel Francois](https://github.com/francoisgaston)
- 62329 - [Lautaro Hernando](https://github.com/laucha12)
